#pragma once

#include <sched.h>

#include <chrono>
#include <functional>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <string>

#include "romulus/cfg.h"
#include "romulus/common.h"
#include "romulus/connection_manager.h"
#include "romulus/device.h"
#include "romulus/memblock.h"
#include "romulus/qp_pol.h"
#include "romulus/stats.h"
#include "romulus/util.h"

constexpr int kChunkSize = 8;
constexpr int kNumOps = 10000;  // 10,000 ops per type (Read, Write, CAS)
constexpr size_t kChainSize = 16;
constexpr int MAX_PAYLOAD = 1024;

#define INGEST_ARGS(args)                                         \
  uint64_t id = args->uget(romulus::NODE_ID);                     \
  std::string registry_ip = args->sget(romulus::REGISTRY_IP);     \
  auto dev_name = args->sget(romulus::DEV_NAME);                  \
  auto dev_port = args->uget(romulus::DEV_PORT);                  \
  std::string output_file = args->sget(romulus::OUTPUT_FILE);     \
  if (std::filesystem::exists(output_file))                       \
    std::filesystem::remove(output_file);                         \
  std::stringstream ss(args->sget(romulus::REMOTES));             \
  std::string remote;                                             \
  std::vector<std::string> machines;                              \
  while (std::getline(ss, remote, ',')) {                         \
    machines.push_back(remote);                                   \
  }                                                               \
  std::string host = machines.at(id);                             \
  std::vector<std::string> remotes = machines;                    \
  remotes.erase(remotes.begin() + id);                            \
  uint64_t system_size = machines.size();                         \
  ROMULUS_INFO("Node {} of {} is {}", id + 1, system_size, host); \
  std::unordered_map<uint64_t, std::string> mach_map;             \
  for (int n = 0; n < (int)machines.size(); ++n) {                \
    mach_map.emplace(n, machines.at(n));                          \
  }                                                               \
  auto transport = args->sget(romulus::TRANSPORT_TYPE);           \
  uint8_t transport_flag;                                         \
  if (transport == "IB") {                                        \
    transport_flag = IBV_LINK_LAYER_INFINIBAND;                   \
  } else {                                                        \
    transport_flag = IBV_LINK_LAYER_ETHERNET;                     \
  }                                                               \
  auto num_qps = args->uget(romulus::NUM_QP);                     \
  auto policy_str = args->sget(romulus::POLICY);

template <typename ConnectionMap>
void drain_all(ConnectionMap& remote_conns) {
  int total = 0;
  for (auto& conn_vec : remote_conns) {
    for (int c = 0; c < (int)conn_vec.second.size(); ++c) {
      auto* conn = conn_vec.second[c];
      ibv_wc wcs[32];
      int n;
      while ((n = ibv_poll_cq(conn->GetCQ(), 32, wcs)) > 0) {
        total += n;
        ROMULUS_INFO("From node {} on index {}", conn_vec.first, c);
        for (int i = 0; i < n; ++i) {
          uint64_t phase = (wcs[i].wr_id >> 48);
          uint64_t tid = (wcs[i].wr_id >> 32) & 0xFFFF;
          uint64_t iter = wcs[i].wr_id & 0xFFFFFFFF;
          ROMULUS_INFO("Drained: phase={}, tid={}, iter={}, wr_id={:#x}", phase,
                       tid, iter, wcs[i].wr_id);
        }
      }
    }
  }
  if (total > 0) ROMULUS_INFO("Drained {} stragglers", total);
}

inline void pin_thread(int tid) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(tid, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

inline std::tuple<double, double, double, double> calc_stats(
    const std::vector<std::chrono::steady_clock::time_point>& start_times,
    const std::vector<std::chrono::steady_clock::time_point>& end_times) {
  std::vector<double> lats;
  for (size_t i = 0; i < start_times.size(); ++i) {
    if (start_times[i] == std::chrono::steady_clock::time_point() ||
        end_times[i] == std::chrono::steady_clock::time_point()) {
      ROMULUS_INFO("Skipping idx {} due to missing timestamp", i);
      continue;
    }
    lats.push_back(
        std::chrono::duration<double, std::micro>(end_times[i] - start_times[i])
            .count());
  }
  double sum = 0;
  for (const auto& lat : lats) {
    sum += lat;
  }
  double avg = sum / lats.size();
  std::sort(lats.begin(), lats.end());
  double p50 = lats[lats.size() / 2];
  double p99 = lats[static_cast<size_t>(lats.size() * 0.99)];
  double p99_9 = lats[static_cast<size_t>(lats.size() * 0.999)];
  // ROMULUS_INFO("+-------------------------+");
  // ROMULUS_INFO("Avg latency: {} us", avg);
  // ROMULUS_INFO("P50 latency: {} us", p50);
  // ROMULUS_INFO("P99 latency: {} us", p99);
  // ROMULUS_INFO("P99.9 latency: {} us", p99_9);
  // ROMULUS_INFO("+-------------------------+\n");
  return {avg, p50, p99, p99_9};
}

void simple_poll(int tid, std::atomic<bool>& is_running,
                 std::atomic<uint64_t>& completions,
                 romulus::ReliableConnection* remote_conn) {
  pin_thread(tid);
  auto cq_raw = remote_conn->GetCQ();

  while (is_running.load(std::memory_order_relaxed)) {
    ibv_wc wc;
    int num_comps = ibv_poll_cq(cq_raw, 1, &wc);
    if (num_comps == 0) continue;

    if (wc.status != IBV_WC_SUCCESS) {
      ROMULUS_FATAL("Work request (wc={}) failed: {}", wc.wr_id,
                    ibv_wc_status_str(wc.status));
      continue;
    }

    completions.fetch_add(1, std::memory_order_relaxed);
  }
}

void batched_poll(int tid, std::atomic<bool>& is_running,
                  std::atomic<uint64_t>& completions,
                  romulus::ReliableConnection* remote_conn) {
  pin_thread(tid);
  auto cq_raw = remote_conn->GetCQ();

  while (is_running.load(std::memory_order_relaxed)) {
    ibv_wc wc[kChainSize];
    int num_comps = ibv_poll_cq(cq_raw, kChainSize, wc);
    if (num_comps == 0) continue;

    for (int i = 0; i < num_comps; ++i) {
      if (wc[i].status != IBV_WC_SUCCESS) {
        ROMULUS_FATAL("Work request (wc={}) failed: {}", wc[i].wr_id,
                      ibv_wc_status_str(wc[i].status));
        continue;
      }
    }
    completions.fetch_add(num_comps, std::memory_order_relaxed);
  }
}

inline void print_table(size_t sz, double read_avg, double write_avg,
                        double cas_avg) {
  ROMULUS_INFO("+----------+-----------+------------+----------+");
  ROMULUS_INFO("| Size (B) | Read (us) | Write (us) | CAS (us) |");
  ROMULUS_INFO("+----------+-----------+------------+----------+");
  ROMULUS_INFO("| {:>8} | {:>9.2f} | {:>10.2f} | {:>8.2f} |", sz, read_avg,
               write_avg, cas_avg);
  ROMULUS_INFO("+----------+-----------+------------+----------+\n");
}

#define RESET()                \
  start_times.clear();         \
  end_times.clear();           \
  start_times.resize(kNumOps); \
  end_times.resize(kNumOps);   \
  completions.store(0);        \
  drain_all(remote_conns);