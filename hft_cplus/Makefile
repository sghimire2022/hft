CXX      := g++
# O3 + march=native uses AVX/SSE on this CPU
# flto = link-time optimisation (inlines across TUs)
# funroll-loops = unroll tight match loops
# fno-exceptions / fno-rtti = remove exception/RTTI overhead
CXXFLAGS := -std=c++17 -O3 -march=native -mtune=native \
            -flto -funroll-loops \
            -fno-exceptions -fno-rtti \
            -fomit-frame-pointer \
            -Wall -Wextra -pthread
TARGET   := hft_engine_opt
SRC      := main.cpp
HDRS     := trading_engine.h logger.h order_book.h engine.h

.PHONY: all clean run bench

all: $(TARGET)

$(TARGET): $(SRC) $(HDRS)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET)
	@echo "✓ Optimized build OK → ./$(TARGET)"

run: all
	mkdir -p logs
	./$(TARGET)

# bench: run 5 times and show throughput each time
bench: all
	mkdir -p logs
	@echo "=== Running 5 benchmark iterations ==="
	@for i in 1 2 3 4 5; do \
	  rm -f logs/engine.log; \
	  ./$(TARGET) 2>&1 | grep "Throughput\|Avg latency\|Min\|Max\|Wall"; \
	  echo "---"; \
	done

clean:
	rm -f $(TARGET) logs/engine.log