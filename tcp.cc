#include <iostream>
#include <chrono>
#include <cstring>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

const int PORT = 6008;

// Use a large buffer for TCP to maximize throughput (Goodput)
const int TCP_CHUNK_SIZE = 65536; 
const int TCP_TOTAL_CHUNKS = 10000; // ~655 MB total

void log_error(const char *prefix, int err) {
  char buf[1024];
  std::cerr << "[Error] " << prefix << " " 
            << strerror_r(err, buf, sizeof(buf))
            << std::endl;
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
  if (listen(server_fd, 32) < 0) {
    log_error("Error listening:", errno);
    return;
  }

  std::cout << "TCP Server listening on port " << PORT << "...\n";

  // accept a incoming connection that returns a new_socket to communicate with the client
  int client_socket = accept(server_fd, nullptr, nullptr);
  if (client_socket < 0) {
    log_error("Error accepting connection:", errno);
    return;
  }

  char buffer[TCP_CHUNK_SIZE];
  long total_bytes = 0;

  auto start = std::chrono::high_resolution_clock::now();
    
  while (true) {
    int bytes_read = recv(client_socket, buffer, sizeof(buffer), 0);
    if (bytes_read == 0) {
      // Client closed connection gracefully
      break;
    } else if (bytes_read < 0) {
      log_error("recv failed:", errno);
      break;
    }
    total_bytes += bytes_read;
  }
    
  auto end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> elapsed = end - start;
  double throughput_mbps = (total_bytes * 8.0 / 1000000.0) / elapsed.count();
  
  std::cout << "TCP Benchmark Complete.\n";
  std::cout << "Total Data Received: " << total_bytes << " bytes.\n";
  std::cout << "Transfer Time: " << elapsed.count() << " seconds.\n";
  std::cout << "Throughput: " << throughput_mbps << " Mbps\n";

  close(client_socket);
  close(server_fd);
}

void run_tcp_client(const char* ip) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    log_error("Socket creation failed:", errno);
    return;
  }

  sockaddr_in serv_addr{};
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(PORT);

  if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
    std::cerr << "Invalid address or address not supported.\n";
    return;
  }

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