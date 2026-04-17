# Compiler and flags
CXX = g++
CXXFLAGS = -O3 -std=c++11 -Wall

# Target executable name
TARGET = tcp
SRC = tcp.cc

# Default build target
all: $(TARGET)

# Compile the benchmark program
$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)

# Cleanup compiled files
clean:
	rm -f $(TARGET)

.PHONY: all clean