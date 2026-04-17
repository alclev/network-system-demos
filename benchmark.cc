#include <iostream>
#include <chrono>
#include <cstring>
#include <vector>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

const int PORT = 6008;

// Use a large buffer for TCP to maximize throughput
const int TCP_CHUNK_SIZE = 65536; 
const int TCP_TOTAL_CHUNKS = 10000; // ~655 MB total

// Keep UDP payload under typical MTU (1500) to avoid IP fragmentation
const int UDP_PACKET_SIZE = 1400; 
const int UDP_NUM_PACKETS = 100000;
const double UDP_TARGET_MBPS = 1024.0; // Target bitrate for the UDP test - 1GB

void log_error(const char *prefix, int err) {
  char buf[1024];
  std::cerr << "[Error] " << prefix << " " 
            << strerror_r(err, buf, sizeof(buf))
            << std::endl;
}

struct UdpPacket {
    uint32_t seq_num;
    char payload[UDP_PACKET_SIZE - sizeof(uint32_t)];
};

void run_tcp_server() {
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    log_error("Error making server socket: ", errno);
    return;
  }

  // set socket options to use address in use
  int opt = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    log_error("setsockopt(SO_REUSEADDR) failed: ", errno);
    return;
  }

  // create the address to bind with server socket
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(PORT);

  // bind the socket to the address
  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
    log_error("Error binding socket to local address: ", errno);
    return;
  }

  // mark the socket to listen state
  if (listen(server_fd, 32) < 0) {
    log_error("Error listening on socket: ", errno);
    return;
  }

  std::cout << "TCP Server listening on port " << PORT << "...\n";

  // accept a incoming connection that returns a new_socket to communicate with the client
  int client_socket = accept(server_fd, nullptr, nullptr);

  char buffer[TCP_CHUNK_SIZE];
  long total_bytes = 0;

  auto start = std::chrono::high_resolution_clock::now();
  while (true) {
    int bytes_read = recv(client_socket, buffer, TCP_CHUNK_SIZE, 0);
    if (bytes_read <= 0) 
      break;
    total_bytes += bytes_read;
  }
  auto end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> elapsed = end - start;
  double throughput_mbps = (total_bytes * 8.0 / 1000000.0) / elapsed.count();
  
  std::cout << "TCP Benchmark Complete.\n";
  std::cout << "Total Data Received: " << total_bytes << " bytes.\n";
  std::cout << "Transfer Time: " << elapsed.count() << " seconds.\n";
  std::cout << "Throughput (Goodput): " << throughput_mbps << " Mbps\n";

  close(client_socket);
  close(server_fd);
}

void run_tcp_client(const char* ip) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in serv_addr{};
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(PORT);
  inet_pton(AF_INET, ip, &serv_addr.sin_addr);

  if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "TCP Connection Failed\n";
    return;
  }

  std::vector<char> buffer(TCP_CHUNK_SIZE, 'A');

  std::cout << "Sending " << TCP_TOTAL_CHUNKS << " TCP chunks (" 
            << (TCP_CHUNK_SIZE * TCP_TOTAL_CHUNKS) / (1024*1024) << " MB)...\n";

  auto start = std::chrono::high_resolution_clock::now();
  
  for (int i = 0; i < TCP_TOTAL_CHUNKS; i++) {
    send(sock, buffer.data(), TCP_CHUNK_SIZE, 0);
  }
  
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end - start;

  std::cout << "Finished TCP transmission in " << elapsed.count() << " seconds.\n";
  close(sock);
}

void run_udp_server() {
  int server_fd = socket(AF_INET, SOCK_DGRAM, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(PORT);

  bind(server_fd, (struct sockaddr*)&address, sizeof(address));
  
  // Set a timeout so the server doesn't hang forever after client finishes
  struct timeval tv;
  tv.tv_sec = 2;
  tv.tv_usec = 0;
  setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

  std::cout << "UDP Server listening on port " << PORT << "...\n";

  UdpPacket pkt;
  long packets_received = 0;
  bool started = false;
  auto start = std::chrono::high_resolution_clock::now();
  auto end = start;

  while (true) {
    int bytes_read = recvfrom(server_fd, &pkt, sizeof(pkt), 0, nullptr, nullptr);
    if (bytes_read < 0) {
      if (started) 
        break; // Timeout after starting means client is done
      continue;
    }
      
    if (!started) {
      start = std::chrono::high_resolution_clock::now();
      started = true;
    }
    packets_received++;
    end = std::chrono::high_resolution_clock::now();
  }

  std::chrono::duration<double> elapsed = end - start;
  long total_bytes = packets_received * UDP_PACKET_SIZE;
  double throughput_mbps = (total_bytes * 8.0 / 1000000.0) / elapsed.count();
  double loss_percent = 100.0 * (UDP_NUM_PACKETS - packets_received) / UDP_NUM_PACKETS;

  std::cout << "UDP Benchmark Complete.\n";
  std::cout << "Received: " << packets_received << " / " << UDP_NUM_PACKETS << " packets.\n";
  std::cout << "Packet Loss: " << loss_percent << "%\n";
  std::cout << "Total Data: " << total_bytes << " bytes.\n";
  std::cout << "Transfer Time: " << elapsed.count() << " seconds.\n";
  std::cout << "Receive Bandwidth: " << throughput_mbps << " Mbps\n";

  close(server_fd);
}

void run_udp_client(const char* ip) {
  int sock = socket(AF_INET, SOCK_DGRAM, 0);
  sockaddr_in serv_addr{};
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(PORT);
  inet_pton(AF_INET, ip, &serv_addr.sin_addr);

  UdpPacket pkt;
  memset(&pkt, 'B', sizeof(pkt));

  // Calculate timing for rate limiting
  double bytes_per_sec = (UDP_TARGET_MBPS * 1000000.0) / 8.0;
  double time_per_packet = UDP_PACKET_SIZE / bytes_per_sec; // in seconds
  std::cout << "Sending " << UDP_NUM_PACKETS << " UDP packets at target " 
            << UDP_TARGET_MBPS << " Mbps...\n";

  auto start = std::chrono::high_resolution_clock::now();
  
  for (uint32_t i = 0; i < UDP_NUM_PACKETS; i++) {
    pkt.seq_num = i;
    
    // Spin-wait for precise pacing to avoid kernel ENOBUFS drops
    auto next_send_time = start + std::chrono::duration<double>(i * time_per_packet);
    while (std::chrono::high_resolution_clock::now() < next_send_time) {
      // Busy-wait
    }

    sendto(sock, &pkt, sizeof(pkt), 0, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
  }

  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end - start;
  std::cout << "Finished sending in " << elapsed.count() << " seconds.\n";

  close(sock);
}

int main(int argc, char const *argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " [server|client] [tcp|udp] <ip_for_client>\n";
    return -1;
  }

  std::string mode = argv[1];
  std::string proto = argv[2];

  if (mode == "server") {
    if (proto == "tcp") run_tcp_server();
    else if (proto == "udp") run_udp_server();
  } else if (mode == "client" && argc == 4) {
    std::string ip = argv[3];
    if (proto == "tcp") run_tcp_client(ip.c_str());
    else if (proto == "udp") run_udp_client(ip.c_str());
  } else {
    std::cerr << "Invalid arguments.\n";
  }

  return 0;
}