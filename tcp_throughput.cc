#include <iostream>
#include <chrono>
#include <cstring>
#include <vector>
#include <thread>
#include <atomic>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>

const int PORT = 6008;
const int NUM_STREAMS = 4; // Optimization 5: Number of parallel TCP streams

const size_t TARGET_DATA_BYTES = 1024ULL * 1024 * 1024; // 1 GiB
// const size_t TCP_CHUNK_SIZE = 1024; // 1 KB chunks
const size_t TCP_CHUNK_SIZE = 1024 * 1024; // 1 MB chunks

void log_error(const char *prefix, int err) {
  char buf[1024];
  std::cerr << "[Error] " << prefix << " " 
            << strerror_r(err, buf, sizeof(buf))
            << std::endl;
}

// Thread worker to handle a single incoming client connection
void server_receive_worker(int client_socket, std::atomic<long>& total_bytes) {
  std::vector<char> buffer(TCP_CHUNK_SIZE);
  long local_bytes = 0;
    
  // Optimization 2: Disable delayed ACKs on the active socket
  int opt = 1;
  setsockopt(client_socket, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt));

  while (true) {
    int bytes_read = recv(client_socket, buffer.data(), TCP_CHUNK_SIZE, 0);
    if (bytes_read == 0) {
      break; // Client closed
    } else if (bytes_read < 0) {
      log_error("recv failed:", errno);
      break;
    }
    local_bytes += bytes_read;
  }
    
  total_bytes += local_bytes;
  close(client_socket);
}

void run_tcp_server() {
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    log_error("Error making server socket:", errno);
    return;
  }

  // set socket options to use address in use
  int opt = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    log_error("setsockopt(SO_REUSEADDR) failed:", errno);
    return;
  }

  // Optmization 2: Disable delayed ACKs
  if (setsockopt(server_fd, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt)) < 0) {
    log_error("setsockopt(TCP_NODELAY) failed:", errno);
    close(server_fd);
    return;
  }

  // Optimization 3: Increase Receive Buffer Size (4 MB)
  // Must be done before listen() so Window Scaling is negotiated correctly
  int rcvbuf = 4 * 1024 * 1024; 
  if (setsockopt(server_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) < 0) {
    log_error("setsockopt(SO_RCVBUF) failed:", errno);
  }

  // create the address to bind with server socket
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(PORT);

  // bind the socket to the address
  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
    log_error("Error binding socket:", errno);
    return;
  }

  // mark the socket to listen state
  if (listen(server_fd, SOMAXCONN) < 0) {
    log_error("Error listening:", errno);
    return;
  }

  std::cout << "TCP Server listening on port " << PORT << "...\n";
  std::cout << "Expecting " << NUM_STREAMS << " incoming streams.\n";

  std::vector<std::thread> workers;
  std::atomic<long> total_bytes{0};
  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < NUM_STREAMS; ++i) {
    int client_socket = accept(server_fd, nullptr, nullptr);
    if (client_socket < 0) {
      log_error("Error accepting connection:", errno);
      continue;
    }
    
    // Start the timer precisely when the first connection is established
    if (i == 0) {
      start = std::chrono::high_resolution_clock::now();
    }

    workers.emplace_back(server_receive_worker, client_socket, std::ref(total_bytes));
  }

  // Wait for all streams to finish transmitting
  for (auto& t : workers) {
    if (t.joinable()) t.join();
  }
    
  auto end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> elapsed = end - start;
  double throughput_mbps = (total_bytes * 8.0 / 1000000.0) / elapsed.count();
  
  std::cout << "TCP Benchmark Complete.\n";
  std::cout << "Total Data Received: " << total_bytes << " bytes (" 
            << total_bytes / (1024 * 1024) << " MB).\n";
  std::cout << "Transfer Time: " << elapsed.count() << " seconds.\n";
  std::cout << "Aggregate Throughput: " << throughput_mbps << " Mbps\n";

  close(server_fd);
}

// Thread worker to transmit a dedicated chunk of the total payload
void client_send_worker(std::string ip, size_t target_bytes, int stream_id) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    log_error("Socket creation failed:", errno);
    return;
  }

  // Optimization 3: Increase Send Buffer Size (4 MB)
  int sndbuf = 4 * 1024 * 1024; 
  setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

  // Optimization 1: Disable Nagle's algorithm
  int opt_nodelay = 1;
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt_nodelay, sizeof(opt_nodelay));

  sockaddr_in serv_addr{};
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(PORT);
  inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr);

  if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "[Stream " << stream_id << "] Connection Failed\n";
    close(sock);
    return;
  }

  std::vector<char> buffer(TCP_CHUNK_SIZE, 'A');
  size_t total_chunks = target_bytes / TCP_CHUNK_SIZE;
  size_t remaining_bytes = target_bytes % TCP_CHUNK_SIZE;

  // Transmit full chunks
  for (size_t i = 0; i < total_chunks; i++) {
    size_t bytes_sent = 0;
    while (bytes_sent < TCP_CHUNK_SIZE) {
      ssize_t result = send(sock, buffer.data() + bytes_sent, TCP_CHUNK_SIZE - bytes_sent, 0);
      if (result <= 0) {
        log_error("Send failed:", errno);
        close(sock);
        return;
      }
      bytes_sent += result;
    }
  }

  // Transmit remainder
  if (remaining_bytes > 0) {
    size_t bytes_sent = 0;
    while (bytes_sent < remaining_bytes) {
      ssize_t result = send(sock, buffer.data() + bytes_sent, remaining_bytes - bytes_sent, 0);
      if (result <= 0) {
        close(sock);
        return;
      }
      bytes_sent += result;
    }
  }
  
  close(sock);
}

void run_tcp_client(const char* ip) {
  std::cout << "Target Data Size: " << TARGET_DATA_BYTES / (1024 * 1024) << " MB\n";
  std::cout << "Chunk Size: " << TCP_CHUNK_SIZE / (1024 * 1024) << " MB\n";
  std::cout << "Executing Multi-Stream Multiplexing with " << NUM_STREAMS << " threads...\n";

  // Calculate payload distribution
  size_t bytes_per_stream = TARGET_DATA_BYTES / NUM_STREAMS;
  size_t remainder = TARGET_DATA_BYTES % NUM_STREAMS;

  std::vector<std::thread> streams;
  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < NUM_STREAMS; ++i) {
    // Ensure the first stream handles any fractional bytes if the payload isn't perfectly divisible
    size_t send_amount = bytes_per_stream + (i == 0 ? remainder : 0);
    streams.emplace_back(client_send_worker, std::string(ip), send_amount, i);
  }

  // Wait for all worker threads to complete transmission
  for (auto& t : streams) {
    if (t.joinable()) t.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end - start;

  std::cout << "Finished TCP transmission in " << elapsed.count() << " seconds.\n";
}

int main(int argc, char const *argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " [server|client] <ip_for_client>\n";
    return -1;
  }

  std::string mode = argv[1];

  if (mode == "server") {
    run_tcp_server();
  } else if (mode == "client" && argc == 3) {
    std::string ip = argv[2];
    run_tcp_client(ip.c_str());
  } else {
    std::cerr << "Invalid arguments.\n";
  }

  return 0;
}