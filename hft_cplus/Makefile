CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread
TARGET   := hft_engine

SRC  := main.cpp
HDRS := trading_engine.h logger.h order_book.h engine.h

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(SRC) $(HDRS)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET)
	@echo "✓ Build OK → ./$(TARGET)"

run: all
	mkdir -p logs
	./$(TARGET)

clean:
	rm -f $(TARGET)
	rm -f logs/engine.log
