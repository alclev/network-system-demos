#include <iostream>
#include <chrono>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>

const int PORT = 6009;
const int TOTAL_TRANSACTIONS = 100000; // 100k synchronous request/responses

// 32 byte message
struct SmallMessage {
  uint32_t sequence_id;
  char payload_data[28];
};

void log_error(const char *prefix, int err) {
  char buf[1024];
  std::cerr << "[Error] " << prefix << " " 
            << strerror_r(err, buf, sizeof(buf))
            << std::endl;
}

// Helper to ensure strict message framing
bool send_request(int sock, const void* buffer, size_t length) {
  size_t bytes_sent = 0;
  const char* ptr = static_cast<const char*>(buffer);
  while (bytes_sent < length) {
    ssize_t result = send(sock, ptr + bytes_sent, length - bytes_sent, 0);
    if (result <= 0) return false;
    bytes_sent += result;
  }
  return true;
}

bool recv_request(int sock, void* buffer, size_t length) {
    size_t bytes_read = 0;
    char* ptr = static_cast<char*>(buffer);
    while (bytes_read < length) {
      ssize_t result = recv(sock, ptr + bytes_read, length - bytes_read, 0);
      if (result <= 0) return false;
      bytes_read += result;
    }
    return true;
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
  /* if (setsockopt(server_fd, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt)) < 0) {
    log_error("setsockopt(TCP_QUICKACK) failed:", errno);
    close(server_fd);
    return;
  } */

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

  std::cout << "Server listening on port " << PORT << "...\n";

  // accept a incoming connection that returns a new_socket to communicate with the client
  int client_socket = accept(server_fd, nullptr, nullptr);
  if (client_socket < 0) {
    log_error("Error accepting connection:", errno);
    return;
  }

  SmallMessage request;
  SmallMessage response;

  long messages_processed = 0;

  // Server Event Loop: Receive request -> Send response
  while (true) {
    if (!recv_request(client_socket, &request, sizeof(request))) {
      break; // Client disconnected
    }
    
    // Echo back the sequence ID as an acknowledgment
    response.sequence_id = request.sequence_id;
    memset(response.payload_data, 1, sizeof(response.payload_data)); // 1 = Success

    if (!send_request(client_socket, &response, sizeof(response))) {
      break;
    }
    messages_processed++;
  }

  std::cout << "Server closed. Processed " << messages_processed << " transactions.\n";
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

  // Optimization 1: Disable Nagle's algorithm on the sending socket
  /* int opt_nodelay = 1;
  if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt_nodelay, sizeof(opt_nodelay)) < 0) {
    log_error("setsockopt(TCP_NODELAY) failed:", errno);
    close(sock);
    return;
  } */

  SmallMessage request;
  SmallMessage response;

  std::cout << "Starting " << TOTAL_TRANSACTIONS << " synchronous RPC calls...\n";

  auto start = std::chrono::high_resolution_clock::now();
    
  for (uint32_t i = 0; i < TOTAL_TRANSACTIONS; i++) {
    request.sequence_id = i;
    memset(request.payload_data, 0x42, sizeof(request.payload_data));

    // Send the small request
    if (!send_request(sock, &request, sizeof(request))) {
      log_error("Send failed", errno);
      break;
    }

    // Block and wait for the exact response
    if (!recv_request(sock, &response, sizeof(response))) {
      log_error("Receive failed", errno);
      break;
    }

    // Validation
    if (response.sequence_id != i) {
      std::cerr << "Sequence mismatch!\n";
      break;
    }
  }
    
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end - start;

  double tps = TOTAL_TRANSACTIONS / elapsed.count();
  double avg_latency_ms = (elapsed.count() * 1000.0) / TOTAL_TRANSACTIONS;

  std::cout << "\nBenchmark Complete\n";
  std::cout << "Total Transactions: " << TOTAL_TRANSACTIONS << "\n";
  std::cout << "Total Time: " << elapsed.count() << " seconds.\n";
  std::cout << "Transactions Per Second (TPS): " << tps << "\n";
  std::cout << "Average Latency: " << avg_latency_ms << " ms per round-trip\n";

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