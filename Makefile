# Compiler and flags
CXX = g++
CXXFLAGS = -O3 -std=c++17 -Wall

# Target executable name
TARGETS = tcp benchmark

# Default build target
all: $(TARGETS)

# Compile the tcp program
tcp: tcp.cc
	$(CXX) $(CXXFLAGS) -o $@ $^

# Compile the benchmark program
benchmark: benchmark.cc
	$(CXX) $(CXXFLAGS) -o $@ $^

# Cleanup compiled files
clean:
	rm -f $(TARGET)

.PHONY: all clean