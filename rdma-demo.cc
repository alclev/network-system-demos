#include "demo.h"

/// @brief
/// @param argc
/// @param argv
/// @return
int main(int argc, char* argv[]) {
  romulus::INIT();
  auto args = std::make_shared<romulus::ArgMap>();
  args->import(romulus::ARGS);
  args->parse(argc, argv);
  INGEST_ARGS(args);

  // Create a registry to be used for this test, all keys are prefixed with
  // 'SimpleTest'. This registry is used for registering connections and
  // barrier.
  ROMULUS_DEBUG("Setting up registry");
  romulus::ConnectionRegistry registry("SimpleTest", registry_ip);

  // The device is responsible for setting up protection domains, which are
  // associated with chunks of remotely accessible memory.
  ROMULUS_DEBUG("Setting up device and allocating protection domain");
  const std::string kPdName = "SimpleTestPd";
  romulus::Device dev(transport_flag);
  dev.Open(dev_name, dev_port);
  dev.AllocatePd(kPdName);
  struct ibv_pd* pd = dev.GetPd(kPdName);

  // A memory block is a region of remotely accessible memory that is
  // associated with a single connection (i.e., QP). There can be several
  // sub-regions of the memory block, which are known as memory regions.
  const int kBlockSize = MAX_PAYLOAD * system_size;
  const std::string kBlockId = "SimpleTestMemBlock";
  const std::string kMemRegionId = "SimpleTestMemRegion";
  ROMULUS_INFO("Allocating MemBlock {} of size {}", kBlockId, kBlockSize);
  uint8_t* raw = new uint8_t[kBlockSize];
  romulus::MemBlock memblock(kBlockId, pd, raw, kBlockSize);
  ROMULUS_INFO("Registering MemRegion {} of size {}", kMemRegionId, kBlockSize);
  memblock.RegisterMemRegion(kMemRegionId, 0, kBlockSize);

  // A connection manager keeps track of all reliable connections (i.e., QPs)
  // that are connected to each memblock.
  ROMULUS_INFO("Setting up connection manager for node {}", id);
  romulus::ConnectionManager mgr(host, &registry, id, system_size, num_qps);

  // The manager first registers the MemBlocks and indicates which nodes
  // (i.e., `remote`) can access them. Since this is a simple test, we only
  // register a single MemBlock.
  ROMULUS_DEBUG("Registering connections...");
  ROMULUS_ASSERT(mgr.Register(dev, memblock), "Connection registration failed");
  // NB: We wait here to ensure that the barrier key has been written
  //     There is a **possible** race condition here
  sleep(3);
  // Barrier
  mgr.arrive_strict_barrier();
  // And then we connect. In order to have a connection, there must be a peer
  // MemBlock registered on a remote machine with the same Id.
  ROMULUS_INFO("Connecting...");
  ROMULUS_ASSERT(mgr.Connect(memblock), "Failed to connect");
  // Barrier again after we connect
  mgr.arrive_strict_barrier();
  // Create an unconnected MemBlock that is used to transfer data to and from
  // the NIC. Divide it into two portions, a write region and read region. The
  // former is used to define the bytes to write over the network, the latter
  // is used to store incoming reads.
  const std::string kLocalMemBlockId = "StagingBlock";
  const std::string kLocalWriteRegion = "WriteStaging";
  const std::string kLocalReadRegion = "ReadStaging";
  const std::string kLocalCASRegion = "CASStaging";
  uint8_t* local_raw = new uint8_t[MAX_PAYLOAD * 3];
  romulus::MemBlock local_memblock(kLocalMemBlockId, pd, local_raw,
                                   MAX_PAYLOAD * 3);
  local_memblock.RegisterMemRegion(kLocalWriteRegion, 0, MAX_PAYLOAD);
  local_memblock.RegisterMemRegion(kLocalReadRegion, MAX_PAYLOAD, MAX_PAYLOAD);
  local_memblock.RegisterMemRegion(kLocalCASRegion, MAX_PAYLOAD * 2,
                                   MAX_PAYLOAD);

  mgr.arrive_strict_barrier();

  // Before launching operations on all the connections, we cache the addrs
  // and connections
  std::unordered_map<std::string, romulus::RemoteAddr> remote_addrs;
  std::unordered_map<std::string, std::vector<romulus::ReliableConnection*>>
      remote_conns;
  romulus::RemoteAddr remote_addr;
  romulus::ReliableConnection* loopback_conn;
  romulus::RemoteAddr loopback_addr;

  for (auto& m : mach_map) {
    if (m.first == id) {
      mgr.GetRemoteAddr(m.first, kBlockId, kMemRegionId, &loopback_addr);
      loopback_conn = mgr.GetConnection(m.first, 0);
      ROMULUS_ASSERT(loopback_addr.addr_info.addr && loopback_conn,
                     "Error establishing loopback addr or connecion.");
      continue;
    }

    mgr.GetRemoteAddr(m.first, kBlockId, kMemRegionId, &remote_addr);
    remote_addrs.emplace(m.second, remote_addr);

    std::vector<romulus::ReliableConnection*> conns;
    for (int q = 1; q < (int)num_qps + 1; ++q) {
      auto conn = mgr.GetConnection(m.first, q);
      conns.push_back(conn);
    }
    remote_conns.emplace(m.second, conns);
  }
  ROMULUS_ASSERT(loopback_conn,
                 "Loopback connection failed to be established.");

  // Dump the values of our maps
  ROMULUS_INFO("Dumping connection info...");
  ROMULUS_INFO("Loopback connection: {}",
               reinterpret_cast<uintptr_t>(loopback_conn));
  for (auto& c : remote_conns) {
    for (int q = 0; q < (int)c.second.size(); ++q) {
      ROMULUS_INFO("Node: {} QP_id: {} Connection; {} CQ: {}", c.first, q,
                   reinterpret_cast<uintptr_t>(c.second[q]),
                   reinterpret_cast<uintptr_t>(c.second[q]->GetCQ()));
    }
  }
  ROMULUS_DEBUG("Dumping raddr info...");
  for (auto& a : remote_addrs) {
    ROMULUS_INFO("Node: {} raddr:{}", a.first, a.second.addr_info.addr);
  }

  ROMULUS_ASSERT(system_size == 2 && num_qps == 1,
                 "This demo is only designed to run with 2 nodes. Detected "
                 "system size: {}",
                 system_size);

  // Metadata that will be reused for our operations

  // std::atomic<bool> is_running = true;
  std::atomic<uint64_t> completions = 0;
  auto remote_conn = remote_conns[remotes.at(0)][0];
  auto raddr = remote_addrs[remotes.at(0)];
  raddr.addr_info.offset = 0;
  auto local_read = local_memblock.GetAddrInfo(kLocalReadRegion);
  auto local_write = local_memblock.GetAddrInfo(kLocalWriteRegion);
  auto local_cas = local_memblock.GetAddrInfo(kLocalCASRegion);
  local_read.offset = 0;
  local_write.offset = 0;
  local_cas.offset = 0;
  uint64_t wr_id_base = (id << 48);
  // We want kNumOps number of ops for each Read, Write, CAS
  std::vector<std::chrono::steady_clock::time_point> start_times(
      kNumOps, std::chrono::steady_clock::time_point());
  std::vector<std::chrono::steady_clock::time_point> end_times(
      kNumOps, std::chrono::steady_clock::time_point());
  // size_t num_poll_threads = 4;
  std::vector<std::thread> polling_threads;
  std::atomic<bool> is_running = true;
  std::thread poller;
  const size_t num_poll_threads = 4;

  std::vector<romulus::WorkRequest> read_wrs(kNumOps);
  std::vector<romulus::WorkRequest> write_wrs(kNumOps);
  std::vector<romulus::WorkRequest> cas_wrs(kNumOps);
  romulus::WorkRequest read_wr, write_wr, cas_wr;
  std::function<void(size_t)> do_work;
  mgr.arrive_strict_barrier();
  // For brevity, only node0 will be dispatching operations, the rest will be
  // passively exposing their memory and on the barrier at the end

  if (id != 0) goto passive_wait;
  pin_thread(0);

  // ############################# 1. BASIC #############################
  // Notes for first demonstration:
  // - If we want to break with CQ back pressure, we just keep increasing
  // kNumOps until eventually it is overwhelmed Post reads
  do_work = [&](size_t sz = 8) {
    local_read.length = sz;
    raddr.addr_info.length = sz;
    // Build the work request once and reuse it, only changing the length and
    // inline flag
    romulus::WorkRequest::BuildRead(local_read, raddr, wr_id_base, &read_wr);
    for (uint64_t i = 0; i < kNumOps; ++i) {
      start_times[i] = std::chrono::steady_clock::now();
      ROMULUS_ASSERT(remote_conn->Post(&read_wr, 1),
                     "Read: Error posting read");
      ROMULUS_ASSERT(remote_conn->ProcessCompletions(1) == 1,
                     "Read: Error processing completions");
      end_times[i] = std::chrono::steady_clock::now();
    }

    double read_avg = std::get<0>(calc_stats(start_times, end_times));

    RESET();

    // Post writes
    local_write.length = sz;
    raddr.addr_info.length = sz;
    romulus::WorkRequest::BuildWrite(local_write, raddr, wr_id_base, &write_wr);
    for (uint64_t i = 0; i < kNumOps; ++i) {
      start_times[i] = std::chrono::steady_clock::now();
      ROMULUS_ASSERT(remote_conn->Post(&write_wr, 1),
                     "Write: Error posting write");
      ROMULUS_ASSERT(remote_conn->ProcessCompletions(1) == 1,
                     "Write: Error processing completions");
      end_times[i] = std::chrono::steady_clock::now();
    }

    double write_avg = std::get<0>(calc_stats(start_times, end_times));
    RESET();

    // Post CAS
    local_cas.length = 8;
    raddr.addr_info.length = 8;
    romulus::WorkRequest::BuildCAS(local_cas, raddr, 0, 1, wr_id_base, &cas_wr);
    for (uint64_t i = 0; i < kNumOps; ++i) {
      start_times[i] = std::chrono::steady_clock::now();
      ROMULUS_ASSERT(remote_conn->Post(&cas_wr, 1), "CAS: Error posting CAS");
      ROMULUS_ASSERT(remote_conn->ProcessCompletions(1) == 1,
                     "CAS: Error processing completions");
      end_times[i] = std::chrono::steady_clock::now();
    }
    double cas_avg = std::get<0>(calc_stats(start_times, end_times));
    RESET();

    // Print
    print_table(sz, read_avg, write_avg, cas_avg);
  };
  
  // Run the test with increasing payload sizes. We should see that as we increase the payload size,
  // it should not have too much of an effect on the latency. Why?
  for (int sz = 8; sz <= MAX_PAYLOAD; sz *= 2) {
    ROMULUS_INFO("\nRunning basic test with payload size {} bytes...", sz);
    do_work(sz);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // ############################# 2. Per-Batch #############################
  // Notes for second demonstration:
  // - The only semantic difference between this and the first test is that are
  // time per batch to show the effect out our later optimizations that rely on
  // batch processing (e.g., doorbell batching, batch polling)
  ROMULUS_INFO("Starting per-batch test...");
  completions.store(0);
  is_running.store(true, std::memory_order_release);
  poller = std::thread(simple_poll, 1, std::ref(is_running),
                       std::ref(completions), remote_conn);

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // Post reads
  {
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kNumOps; ++i) {
      uint64_t wr_id = wr_id_base | i;
      remote_conn->Read(local_read, raddr, wr_id);
    }
    while (completions.load(std::memory_order_relaxed) < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO("Processed {} reads in {} ms ({:.2f} Mops/sec)", kNumOps,
                 elapsed_ms, kNumOps / elapsed_s / 1e6);
    RESET();
  }

  // Post writes
  {
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kNumOps; ++i) {
      uint64_t wr_id = wr_id_base | i;
      remote_conn->Write(local_write, raddr, wr_id);
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO("Processed {} writes in {} ms ({:.2f} Mops/sec)", kNumOps,
                 elapsed_ms, kNumOps / elapsed_s / 1e6);
    RESET();
  }

  // Post CAS
  {
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kNumOps; ++i) {
      uint64_t wr_id = wr_id_base | i;
      auto expected = i;
      auto swap = i + 1;
      remote_conn->CompareAndSwap(local_cas, raddr, expected, swap, wr_id);
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO("Processed {} CAS operations in {} ms ({:.2f} Mops/sec)",
                 kNumOps, elapsed_ms, kNumOps / elapsed_s / 1e6);
    RESET();
  }
  // Clean resources...
  is_running.store(false, std::memory_order_release);
  poller.join();

  // ###################### 3. Increased Polling Threads ######################
  // Notes for third demonstration:
  // - Here we show the effect of increasing the number of polling threads on
  // the system's ability to keep up with completions. We can see that with more
  // polling threads, it doesn't always translate to lower latency. Why?

  ROMULUS_INFO("\nStarting increased polling threads test...");
  is_running.store(true, std::memory_order_release);
  for (size_t t = 0; t < num_poll_threads; ++t) {
    polling_threads.emplace_back(simple_poll, 1 + t, std::ref(is_running),
                                 std::ref(completions), remote_conn);
  }
  // Give threads time to spin up and start polling
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // Post reads
  {
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kNumOps; ++i) {
      uint64_t wr_id = wr_id_base | i;
      remote_conn->Read(local_read, raddr, wr_id);
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO("Processed {} reads in {} ms ({:.2f} Mops/sec)", kNumOps,
                 elapsed_ms, kNumOps / elapsed_s / 1e6);
    RESET();
  }

  // Post writes
  {
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kNumOps; ++i) {
      uint64_t wr_id = wr_id_base | i;
      remote_conn->Write(local_write, raddr, wr_id);
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO("Processed {} writes in {} ms ({:.2f} Mops/sec)", kNumOps,
                 elapsed_ms, kNumOps / elapsed_s / 1e6);
    RESET();
  }

  // Post CAS
  {
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kNumOps; ++i) {
      uint64_t wr_id = wr_id_base | i;
      auto expected = i;
      auto swap = i + 1;
      remote_conn->CompareAndSwap(local_cas, raddr, expected, swap, wr_id);
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO("Processed {} CAS operations in {} ms ({:.2f} Mops/sec)",
                 kNumOps, elapsed_ms, kNumOps / elapsed_s / 1e6);
    RESET();
  }

  // Clean resources...
  is_running.store(false, std::memory_order_release);
  for (auto& t : polling_threads) {
    t.join();
  }
  polling_threads.clear();

  // ######################## 4. Batched Polling ########################
  // Notes for fourth demonstration:
  // - Here we show the effect of batching completions in the polling threads.
  // - Here we should see a significant improvement in the system's ability to
  // keep up with completions. Why?
  ROMULUS_INFO("\nStarting batched polling test...");
  is_running.store(true, std::memory_order_release);
  for (size_t t = 0; t < num_poll_threads; ++t) {
    polling_threads.emplace_back(batched_poll, 1 + t, std::ref(is_running),
                                 std::ref(completions), remote_conn);
  }

  // Give threads time to spin up and start polling
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Post reads
  {
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kNumOps; ++i) {
      uint64_t wr_id = wr_id_base | i;
      remote_conn->Read(local_read, raddr, wr_id);
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO(
        "Processed {} reads in {} ms ({:.2f} Mops/sec) ({} ibv_post_send)",
        kNumOps, elapsed_ms, kNumOps / elapsed_s / 1e6, completions.load());
    RESET();
  }

  // Post writes
  {
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kNumOps; ++i) {
      uint64_t wr_id = wr_id_base | i;
      remote_conn->Write(local_write, raddr, wr_id);
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO(
        "Processed {} writes in {} ms ({:.2f} Mops/sec) ({} ibv_post_send)",
        kNumOps, elapsed_ms, kNumOps / elapsed_s / 1e6, completions.load());
    RESET();
  }

  // Post CAS
  {
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kNumOps; ++i) {
      uint64_t wr_id = wr_id_base | i;
      auto expected = i;
      auto swap = i + 1;
      remote_conn->CompareAndSwap(local_cas, raddr, expected, swap, wr_id);
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    double elapsed_s = std::chrono::duration<double>(end - start).count();
    double elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO(
        "Processed {} CAS operations in {} ms ({:.2f} Mops/sec) ({} "
        "ibv_post_send)",
        kNumOps, elapsed_ms, kNumOps / elapsed_s / 1e6, completions.load());
    RESET();
  }
  // Clean resources...
  is_running.store(false, std::memory_order_release);
  for (auto& t : polling_threads) {
    t.join();
  }
  polling_threads.clear();

  // ################# 5. Batched Polling + Doorbell Batching ################
  // Notes for fifth demonstration:
  // - Here we show the effect of batching completions in the polling threads
  // along with batching work requests on the doorbell. We should see a
  // significant improvement in throughput as we decrease the number of doorbell
  // rings and increase the efficiency of polling.
  ROMULUS_INFO("\nStarting batched polling + doorbell batching test...");
  is_running.store(true, std::memory_order_release);
  for (size_t t = 0; t < num_poll_threads; ++t) {
    polling_threads.emplace_back(batched_poll, 1 + t, std::ref(is_running),
                                 std::ref(completions), remote_conn);
  }

  // Give threads time to spin up and start polling
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Construct the batches
  for (size_t batch_start = 0; batch_start < kNumOps;
       batch_start += kChainSize) {
    size_t batch_size = std::min(kChainSize, kNumOps - batch_start);

    for (size_t i = 0; i < batch_size; ++i) {
      size_t idx = batch_start + i;
      romulus::WorkRequest::BuildRead(local_read, raddr, wr_id_base | idx,
                                      &read_wrs[idx]);
      romulus::WorkRequest::BuildWrite(local_write, raddr, wr_id_base | idx,
                                       &write_wrs[idx]);
      auto expected = idx;
      auto swap = idx + 1;
      romulus::WorkRequest::BuildCAS(local_cas, raddr, expected, swap,
                                     wr_id_base | idx, &cas_wrs[idx]);
      if (i > 0) {
        read_wrs[idx - 1].append(&read_wrs[idx]);
        write_wrs[idx - 1].append(&write_wrs[idx]);
        cas_wrs[idx - 1].append(&cas_wrs[idx]);
      }
    }
  }

  // Now post each chain
  {
    auto start = std::chrono::steady_clock::now();
    for (size_t batch_start = 0; batch_start < kNumOps;
         batch_start += kChainSize) {
      size_t batch_size = std::min(kChainSize, kNumOps - batch_start);
      romulus::WorkRequest* chain_head = &read_wrs[batch_start];
      ROMULUS_ASSERT(remote_conn->Post(chain_head, batch_size),
                     "Failed to post chain");
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO(
        "Processed {} reads in {} ms ({:.2f} Mops/sec) ({} ibv_post_send) with "
        "doorbell batching",
        kNumOps, elapsed_ms, kNumOps / elapsed_s / 1e6, kNumOps / kChainSize);
    RESET();
  }

  // Post reads
  {
    auto start = std::chrono::steady_clock::now();
    for (size_t batch_start = 0; batch_start < kNumOps;
         batch_start += kChainSize) {
      size_t batch_size = std::min(kChainSize, kNumOps - batch_start);
      romulus::WorkRequest* chain_head = &write_wrs[batch_start];
      ROMULUS_ASSERT(remote_conn->Post(chain_head, batch_size),
                     "Failed to post chain");
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO(
        "Processed {} writes in {} ms ({:.2f} Mops/sec) ({} ibv_post_send) "
        "with doorbell batching",
        kNumOps, elapsed_ms, kNumOps / elapsed_s / 1e6, kNumOps / kChainSize);
    RESET();
  }

  // Post CAS
  {
    auto start = std::chrono::steady_clock::now();
    for (size_t batch_start = 0; batch_start < kNumOps;
         batch_start += kChainSize) {
      size_t batch_size = std::min(kChainSize, kNumOps - batch_start);
      romulus::WorkRequest* chain_head = &cas_wrs[batch_start];
      ROMULUS_ASSERT(remote_conn->Post(chain_head, batch_size),
                     "Failed to post chain");
    }
    while (completions.load() < kNumOps) {
      std::this_thread::yield();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_s = std::chrono::duration<double>(end - start).count();
    auto elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    ROMULUS_INFO(
        "Processed {} CAS operations in {} ms ({:.2f} Mops/sec) ({} "
        "ibv_post_send) with doorbell "
        "batching",
        kNumOps, elapsed_ms, kNumOps / elapsed_s / 1e6, kNumOps / kChainSize);
    RESET();
  }
  // Clean resources...
  is_running.store(false, std::memory_order_release);
  for (auto& t : polling_threads) {
    t.join();
  }
  polling_threads.clear();

passive_wait:
  mgr.arrive_strict_barrier();

  ROMULUS_INFO("Done. Cleaning up...");
  delete[] raw;
  delete[] local_raw;
}
