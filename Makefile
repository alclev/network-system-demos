# Compiler and flags
CXX = g++
CXXFLAGS = -O3 -std=c++17 -Wall -lpthread

# Target executable name
TARGETS = tcp_throughput tcp_latency tcp_distributed benchmark

# Default build target
all: $(TARGETS)

tcp_throughput: tcp_throughput.cc
	$(CXX) $(CXXFLAGS) -o $@ $^

tcp_latency: tcp_latency.cc
	$(CXX) $(CXXFLAGS) -o $@ $^

tcp_distributed: tcp_distributed.cc
	$(CXX) $(CXXFLAGS) -o $@ $^	

benchmark: benchmark.cc
	$(CXX) $(CXXFLAGS) -o $@ $^

# Cleanup compiled files
clean:
	rm -f $(TARGETS)

.PHONY: all clean