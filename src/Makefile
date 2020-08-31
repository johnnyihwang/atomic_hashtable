CFLAGS = -Wall -std=c++17 -lpthread -fPIC
FILES =  atomic_hashtable.h
BENCH_FILES = hash_bench.cpp
OUTPUT = hash_bench

all: run

debug: $(FILES) $(BENCH_FILES)
	g++ $(CFLAGS) -g -O0 $^ -o $(OUTPUT)

run: $(FILES) $(BENCH_FILES)
	g++ $(CFLAGS) -O3 $^ -o $(OUTPUT)

.PHONY: clean
clean:
	rm -rf *.o *~ hash_bench
