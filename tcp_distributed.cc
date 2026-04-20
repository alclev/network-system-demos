#include <iostream>
#include <thread>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <chrono>
#include <atomic>
#include <string>
#include <sys/uio.h>
#include <netinet/tcp.h>

// Node configuration
const int NUM_CLIENT_THREADS = 6;
const int PORT = 6005;
const int NUM_NODES = 4;
const int TOTAL_MESSAGES = 120000;
const int MESSAGES_PER_THREAD = TOTAL_MESSAGES / NUM_CLIENT_THREADS;
const int BATCH_SIZE = 100;

const int BARRIER_PORT_START = 6006;
const int BARRIER_PORT_END = 6007;

// Hardcoded Cluster Routing Table
const std::vector<std::string> node_ips = {
  "128.180.120.87", // Node 0: Saturn
  "128.180.120.75", // Node 1: Hydra
  "128.180.120.70", // Node 2: Cupid
  "128.180.120.80"  // Node 3: Mercury
};

// Optimization 1: Memory Alignment to Cache Lines (64 bytes)
struct alignas(64) Message {
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

// Optimization 2: TCP Socket Tuning
void apply_tcp_optimizations(int sock) {
  int opt = 1;
  // Disable Nagle's Algorithm to prevent payload buffering delays
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    
  // Disable Delayed ACKs to speed up the TCP sliding window
  setsockopt(sock, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt));
}

/* // Helper function to guarantee full reads
ssize_t read_exact(int fd, void* buf, size_t count) {
  size_t total_read = 0;
  char* char_buf = static_cast<char*>(buf);
  while (total_read < count) {
    ssize_t bytes = read(fd, char_buf + total_read, count - total_read);
    if (bytes <= 0) {
        return bytes; // 0 for EOF, -1 for error
    }
    total_read += bytes;
  }
  return total_read;
} */

// Optimization 3: Zero-Copy/Gather I/O Exact Reader
ssize_t readv_exact(int fd, struct iovec* iov, int iovcnt, size_t total_expected) {
  size_t total_read = 0;
  while (total_read < total_expected) {
    ssize_t bytes = readv(fd, iov, iovcnt);
    if (bytes <= 0) 
      return bytes; 
        
    total_read += bytes;
    size_t bytes_to_consume = bytes;
        
    // Advance the iovec array pointers for partial reads
    for (int i = 0; i < iovcnt; ++i) {
      if(bytes_to_consume == 0)
        break;
      
      if (iov[i].iov_len > 0) {
        if (bytes_to_consume >= iov[i].iov_len) {
          bytes_to_consume -= iov[i].iov_len;
          iov[i].iov_base = nullptr;
          iov[i].iov_len = 0;
        } else {
            iov[i].iov_base = static_cast<char*>(iov[i].iov_base) + bytes_to_consume;
            iov[i].iov_len -= bytes_to_consume;
            bytes_to_consume = 0;
        }
      }
    }
  }
  return total_read;
}

/* // Helper function to guarantee full writes
ssize_t write_exact(int fd, const void* buf, size_t count) {
  size_t total_written = 0;
  const char* char_buf = static_cast<const char*>(buf);
  while (total_written < count) {
    ssize_t bytes = write(fd, char_buf + total_written, count - total_written);
    if (bytes <= 0) {
        return bytes; 
    }
    total_written += bytes;
  }
  return total_written;
} */

// Optimization 3: Zero-Copy/Scatter I/O Exact Writer
ssize_t writev_exact(int fd, struct iovec* iov, int iovcnt, size_t total_expected) {
  size_t total_written = 0;
  while (total_written < total_expected) {
    ssize_t bytes = writev(fd, iov, iovcnt);
    if (bytes <= 0)
      return bytes;
    
    total_written += bytes;
    size_t bytes_to_consume = bytes;
    
    // Advance the iovec array pointers for partial writes
    for (int i = 0; i < iovcnt; ++i) {
      if (bytes_to_consume == 0) break;
      if (iov[i].iov_len > 0) {
        if (bytes_to_consume >= iov[i].iov_len) {
          bytes_to_consume -= iov[i].iov_len;
          iov[i].iov_base = nullptr;
          iov[i].iov_len = 0;
        } else {
          iov[i].iov_base = static_cast<char*>(iov[i].iov_base) + bytes_to_consume;
          iov[i].iov_len -= bytes_to_consume;
          bytes_to_consume = 0;
        }
      }
    }
  }
  return total_written;
}

// Helper to reset iovec arrays between read/write cycles
void reset_iovec(struct iovec* iov, Message* msgs, int count) {
  for (int i = 0; i < count; ++i) {
    iov[i].iov_base = &msgs[i];
    iov[i].iov_len = sizeof(Message);
  }
}

std::atomic<int> messages_processed{0};

/* // Server: Handles incoming requests
void server_worker(int client_sock) {
  Message msg;
  while (read_exact(client_sock, &msg, sizeof(Message)) > 0) {
    messages_processed++;
    
    // Simulate processing
    msg.sender_id = -1; // ACK
    
    // Standard write() copies data from User Space to Kernel Space
    write_exact(client_sock, &msg, sizeof(Message));
  }
  close(client_sock);
} */

// Server: Handles incoming requests
void server_worker(int client_sock) {
  apply_tcp_optimizations(client_sock);

  Message msgs[BATCH_SIZE];
  struct iovec iov[BATCH_SIZE];
  size_t batch_bytes = sizeof(Message) * BATCH_SIZE;

  while (true) {
    reset_iovec(iov, msgs, BATCH_SIZE);
    
    // Re-enable QUICKACK per recv cycle (Linux kernel resets this flag dynamically)
    int opt = 1;
    setsockopt(client_sock, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt));

    ssize_t read_bytes = readv_exact(client_sock, iov, BATCH_SIZE, batch_bytes);
    if (read_bytes <= 0) break;

    messages_processed += BATCH_SIZE;

    // Simulate processing (ACK)
    for (int i = 0; i < BATCH_SIZE; ++i) {
        msgs[i].sender_id = -1; 
    }

    reset_iovec(iov, msgs, BATCH_SIZE);
    writev_exact(client_sock, iov, BATCH_SIZE, batch_bytes);
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
  /* if (listen(server_fd, 128) < 0) {
    log_error("Error listening:", errno);
    return;
  } */
  if (listen(server_fd, 1024) < 0) {
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

/* // Client: Sends requests to other nodes
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
    write_exact(sock, &msg, sizeof(Message));
      
    Message response;
    read_exact(sock, &response, sizeof(Message));

    // Closing connection immediately after one request/response
    close(sock);
  }
} */

// Client: Sends requests to other nodes
void client_thread(int my_node_id, int target_node_id) {
  // Optimization 4: Persistent Connection Pool (1 Socket per worker thread)
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return;

  sockaddr_in serv_addr{};
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(PORT);
  inet_pton(AF_INET, node_ips[target_node_id].c_str(), &serv_addr.sin_addr);

  if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    close(sock);
    return;
  }

  apply_tcp_optimizations(sock);

  Message msgs[BATCH_SIZE];
  struct iovec iov[BATCH_SIZE];
  size_t batch_bytes = sizeof(Message) * BATCH_SIZE;

  int num_batches = MESSAGES_PER_THREAD / BATCH_SIZE;

  for (int b = 0; b < num_batches; ++b) {
    // Prepare Batch
    for (int i = 0; i < BATCH_SIZE; ++i) {
        msgs[i].sender_id = my_node_id;
        msgs[i].message_id = (b * BATCH_SIZE) + i;
        memset(msgs[i].payload, 'A', sizeof(msgs[i].payload));
    }

    // Scatter/Gather Write
    reset_iovec(iov, msgs, BATCH_SIZE);
    writev_exact(sock, iov, BATCH_SIZE, batch_bytes);

    // Scatter/Gather Read
    reset_iovec(iov, msgs, BATCH_SIZE);
    readv_exact(sock, iov, BATCH_SIZE, batch_bytes);
  }
  
  close(sock);
}

void distributed_barrier(int my_node_id, int num_nodes, const std::string& coordinator_ip, int barrier_port) {
  // Basic exact IO helpers for the barrier
  auto write_exact_barrier = [](int fd, const void* buf, size_t count) {
    size_t total = 0;
    const char* p = static_cast<const char*>(buf);
    while(total < count) {
      ssize_t b = write(fd, p + total, count - total);
      if(b <= 0) return b;
      total += b;
    }
    return (ssize_t)total;
  };
  auto read_exact_barrier = [](int fd, void* buf, size_t count) {
    size_t total = 0;
    char* p = static_cast<char*>(buf);
    while(total < count) {
      ssize_t b = read(fd, p + total, count - total);
      if(b <= 0) return b;
      total += b;
    }
    return (ssize_t)total;
  };

  if (my_node_id == 0) {
    // Node 0 acts as the coordinator
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
      
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(barrier_port);
      
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
      log_error("Barrier bind failed", errno);
      exit(1);
    }
    listen(server_fd, num_nodes);
      
    std::cout << "[Node 0] Coordinator waiting at barrier on port " << barrier_port << "...\n";
    
    std::vector<int> client_sockets;
    for (int i = 0; i < num_nodes - 1; ++i) {
      int client_sock = accept(server_fd, nullptr, nullptr);
      char ready;
      read_exact_barrier(client_sock, &ready, 1); // Receive 'R'
      client_sockets.push_back(client_sock);
    }
      
    // Broadcast 'G' (Go) to release all nodes
    for (int sock : client_sockets) {
      char go = 'G';
      write_exact_barrier(sock, &go, 1);
      close(sock);
    }
    close(server_fd);
  } else {
    // Nodes 1-3 act as participants
    int sock;
    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(barrier_port);
    inet_pton(AF_INET, coordinator_ip.c_str(), &serv_addr.sin_addr);
    
    std::cout << "[Node " << my_node_id << "] Reaching barrier on port " << barrier_port << "...\n";
    
    // Retry loop in case Node 0's socket isn't actively listening yet
    while (true) {
      sock = socket(AF_INET, SOCK_STREAM, 0);
      if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == 0) {
          break;
      }
      close(sock);
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    char ready = 'R';
    write_exact_barrier(sock, &ready, 1); // Send Ready
    
    char go;
    read_exact_barrier(sock, &go, 1);     // Block until Go is received
    close(sock);
  }
  std::cout << "[Node " << my_node_id << "] Barrier passed.\n";
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

  distributed_barrier(my_node_id, NUM_NODES, node_ips[0], BARRIER_PORT_START);

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

  distributed_barrier(my_node_id, NUM_NODES, node_ips[0], BARRIER_PORT_END);

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end_time - start_time;

  std::cout << "Node " << my_node_id << " finished sending. " 
            << "Time taken: " << elapsed.count() << " seconds.\n";
  std::cout << "Total messages processed by server: " << messages_processed << "\n";

  return 0;
}