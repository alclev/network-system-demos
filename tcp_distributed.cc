#include <iostream>
#include <thread>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <chrono>
#include <atomic>
#include <string>

// Node configuration
const int NUM_CLIENT_THREADS = 6;
const int PORT = 6005;
const int NUM_NODES = 4;
const int MESSAGES_PER_THREAD = 100000;

// Hardcoded Cluster Routing Table
const std::vector<std::string> node_ips = {
  "128.180.120.87", // Node 0
  "128.180.120.75", // Node 1 
  "128.180.120.70", // Node 2
  "128.180.120.80"  // Node 3
};

struct Message {
  int sender_id;
  int message_id;
  char payload[1024];
};

void log_error(const char *prefix, int err) {
  char buf[1024];
  std::cerr << "[Error] " << prefix << " " 
            << strerror_r(err, buf, sizeof(buf))
            << std::endl;
}

std::atomic<int> messages_processed{0};

// Server: Handles incoming requests
void server_worker(int client_sock) {
  Message msg;
  while (read(client_sock, &msg, sizeof(Message)) > 0) {
    messages_processed++;
    
    // Simulate processing
    msg.sender_id = -1; // ACK
    
    // Standard write() copies data from User Space to Kernel Space
    write(client_sock, &msg, sizeof(Message));
  }
  close(client_sock);
}

void server_loop(int node_id) {
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
  if (listen(server_fd, 128) < 0) {
    log_error("Error listening:", errno);
    return;
  }

  std::cout << "Server listening on port " << PORT << "...\n";

  while (true) {
    int client_sock = accept(server_fd, nullptr, nullptr);
    if (client_sock < 0) {
      log_error("Error accepting connection:", errno);
      return;
    }
    // Spawns a new thread per connection
    std::thread(server_worker, client_sock).detach();
  }
}

// Client: Sends requests to other nodes
void client_thread(int my_node_id, int target_node_id) {
  for (int i = 0; i < MESSAGES_PER_THREAD; ++i) {
      
    // Opening a new TCP connection for every message
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        continue;
    }

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    if (inet_pton(AF_INET, node_ips[target_node_id].c_str(), &serv_addr.sin_addr) <= 0) {
        close(sock);
        continue;
    }

    // Establish 3-way TCP handshake
    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
      close(sock);
      continue;
    }

    Message msg;
    msg.sender_id = my_node_id;
    msg.message_id = i;
    memset(msg.payload, 'A', sizeof(msg.payload));

    // Unoptimized payload delivery: 1 write per syscall
    write(sock, &msg, sizeof(Message));
      
    Message response;
    read(sock, &response, sizeof(Message));

    // Closing connection immediately after one request/response
    close(sock);
  }
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: ./tcp_distributed <my_id>\n";
    return 1;
  }

  int my_node_id = std::stoi(argv[1]);
  
  // Safety check to ensure valid node ID
  if (my_node_id < 0 || my_node_id >= NUM_NODES) {
    std::cerr << "Invalid node ID. Must be between 0 and 3.\n";
    return 1;
  }
  
  // Start Server Thread
  std::thread server(server_loop, my_node_id);
  server.detach();

  // Wait for all nodes to spin up their servers
  std::this_thread::sleep_for(std::chrono::seconds(2));

  auto start_time = std::chrono::high_resolution_clock::now();

  // Start Client Threads
  std::vector<std::thread> clients;
  for (int i = 0; i < NUM_CLIENT_THREADS; ++i) {
    // Distribute load across other nodes
    int target_node = (my_node_id + 1 + (i % (NUM_NODES - 1))) % NUM_NODES;
    clients.emplace_back(client_thread, my_node_id, target_node);
  }

  for (auto& t : clients) {
    t.join();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end_time - start_time;

  std::cout << "Node " << my_node_id << " finished sending. " 
            << "Time taken: " << elapsed.count() << " seconds.\n";

  // Keep alive to process remaining incoming messages
  std::this_thread::sleep_for(std::chrono::seconds(5));
  std::cout << "Total messages processed by server: " << messages_processed << "\n";

  return 0;
}