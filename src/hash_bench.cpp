#include <cstdlib>
#include <cstdint>
#include <unistd.h>
#include <iostream>
#include <atomic>
#include <vector>
#include <thread> 
#include <chrono>
#include <unordered_map>
#include <cassert>
#include "atomic_hashtable.h"

using namespace std;

void print_help() {
    cout << "Usage: hash_bench [OPTIONS]\n\n";
    cout << "Options:\n";

    cout << "  -h, --help\tPrint this help message.\n";
    cout << "  -n\t\tNumber of iterations for which items will be inserted in each experiment. At each iteration, 3 inserts will be performed.\n";
    cout << "\t\t    Default: 100000\n";
    cout << "  -i\t\tNumber of times that the iterations will be increased in order to test a larger number of inserts. At each increase, the number of iterations will double.\n";
    cout << "\t\t    Default: 4\n";
    cout << "  -t\t\tMaximum number of threads to spawn.\n";
    cout << "\t\t    Default: 6\n";
    cout << "  -r\t\tNumber of repeated experiments for each number of iterations/number of threads. The average throughput over this number of experiments will be reported.\n";
    cout << "\t\t    Default: 5\n";
    cout << "  -d\t\tAtomic hash table delete threshold. When more entries than the given number are deleted, the hash table will lock and reclaim memory.\n";
    cout << "\t\t    Default: 5000\n";
    cout << "  -v\t\tIf this flag is given, the runtime (in ms) for each experiment will be printed out.\n";
    cout << "\n";
}

void thread_job_lookup_init(atomic_hashtable::SplitOrderedHash<int,int> *hasht, int limit) {
    for (int i = 0; i < limit * 10; i++)
        hasht->insert(i,limit-i);
}

void thread_job_lookup(atomic_hashtable::SplitOrderedHash<int,int> *hasht, int limit) {
    for (int i = 0; i < limit * 10; i++)
        assert(hasht->get(i) == limit-i);
}

void thread_job_erase(atomic_hashtable::SplitOrderedHash<int,int> *hasht, int limit) {
    for(int i = 0; i < limit; i++) {
        int key = rand();
        
        if(hasht->contains(key)) {
            hasht->erase(key);
        }
        hasht->insert(key,i);
        if(hasht->contains(key)) {
            hasht->erase(key);
        }
        int j = key + (limit/2);
        hasht->insert(j,i);
    }
}

void thread_job_insert(atomic_hashtable::SplitOrderedHash<int,int> *hasht, int limit) {
    for(int i = 0; i < limit; i++) {
        int key = rand();
        hasht->insert(key,i);
        hasht->insert(key+limit,i);
        hasht->insert(key+limit+limit,i);
    }
}

void run_lookup_test(const unsigned int limit, const uint64_t delete_limit, const unsigned int thread_num, bool print_time,
                    uint64_t& total_time, double& throughput)
{
    atomic_hashtable::SplitOrderedHash<int,int> *hasht = new atomic_hashtable::SplitOrderedHash<int,int>();
    hasht->change_delete_cleanup_threshold(delete_limit);
    thread_job_lookup_init(hasht, limit);
    vector<thread> threads(thread_num);
    auto start = chrono::high_resolution_clock::now(); 
    for(unsigned int i = 0; i < thread_num; i++)
        threads.push_back(thread(thread_job_lookup, hasht, limit));
    for(thread &th : threads) {
        if (th.joinable())
            th.join();
    }
    auto end = chrono::high_resolution_clock::now(); 
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    if (print_time)
        cout << duration.count() << " ms\n";
    total_time += duration.count();
    throughput += static_cast<double>(limit * 10) / duration.count();

    delete hasht;
}


void run_insert_test(const unsigned int limit, const uint64_t delete_limit, const unsigned int thread_num, bool print_time,
                    uint64_t& total_time, double& throughput)
{
    atomic_hashtable::SplitOrderedHash<int,int> *hasht = new atomic_hashtable::SplitOrderedHash<int,int>();
    hasht->change_delete_cleanup_threshold(delete_limit);
    vector<thread> threads(thread_num);
    auto start = chrono::high_resolution_clock::now(); 
    for(unsigned int i = 0; i < thread_num; i++)
        threads.push_back(thread(thread_job_insert, hasht, limit));
    for(thread &th : threads) {
        if (th.joinable())
            th.join();
    }
    auto end = chrono::high_resolution_clock::now(); 
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    if (print_time)
        cout << duration.count() << " ms\n";
    total_time += duration.count();
    throughput += static_cast<double>(limit * 3) / duration.count();

    delete hasht;
}

void run_deletion_test(const unsigned int limit, const uint64_t delete_limit, const unsigned int thread_num, bool print_time,
                    uint64_t& total_time, double& throughput)
{
    unsigned int thread_num_half = thread_num / 2;
    atomic_hashtable::SplitOrderedHash<int,int> *hasht = new atomic_hashtable::SplitOrderedHash<int,int>();
    hasht->change_delete_cleanup_threshold(delete_limit);
    vector<thread> threads(thread_num_half);
    auto start = chrono::high_resolution_clock::now(); 
    for(unsigned int i = 0; i < thread_num_half; i++) {
        threads.push_back(thread(thread_job_insert, hasht, limit));
        threads.push_back(thread(thread_job_erase, hasht, limit));
    }
    for(thread &th : threads) {
        if (th.joinable())
            th.join();
    }
    auto end = chrono::high_resolution_clock::now(); 
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    if (print_time)
        cout << duration.count() << " ms\n";
    total_time += duration.count();
    throughput += static_cast<double>(limit * 3) / duration.count();

    delete hasht;
}

int main(int argc, char **argv) {
    if(argc == 2 && strcmp(argv[1], "--help")==0) {
        print_help();
        return 0;
    }
    
    unsigned int limit = 100000;
    unsigned int limit_increase_iter = 4;
    unsigned int max_thread_num = 6;
    unsigned int repeat = 5;
    bool print_time = false;
    uint64_t delete_limit = 5000;
    
    // Command line argument parsing
    int c;
    unsigned int tmp;
    while ((c = getopt (argc, argv, "hn:i:t:r:d:v")) != -1) {
        switch (c) {
            case 'h':
                print_help();
                return 0;
            case 'n':
                tmp = stoi(optarg);
                limit = tmp < 1 ? 1 : tmp;
                break;
            case 'i':
                tmp = stoi(optarg);
                limit_increase_iter = tmp < 1 ? 1 : tmp;
                break;
            case 't':
                tmp = stoi(optarg);
                max_thread_num = tmp < 1 ? 1 : tmp;
                break;
            case 'r' :
                tmp = stoi(optarg);
                repeat = tmp < 1 ? 1 : tmp;
                break;
            case 'd' :
                tmp = stoi(optarg);
                delete_limit = tmp < 1 ? 1 : tmp;
                break;
            case 'v' :
                print_time = true;
                break;
            case '?':
                print_help();
                return 1;
            default:
                abort();
        }
    }

    // Basic functionality check & atomic operations support check
    atomic_hashtable::SplitOrderedHash<int,int> hasht_local(rand() % 10000);
    if(!hasht_local.is_lock_free()) {
        cout << "Atomic operations not supported by this platform, quitting benchmark!\n";
        return 0;
    }

    cout << "Checking basic functionalities...\n";
    hasht_local.change_resize_threshold(1.5);
    hasht_local.change_delete_cleanup_threshold(30);
    for(unsigned int i = 0; i < limit; i++)
        hasht_local.insert(i,static_cast<int>(limit-i));

    for(unsigned int i = 0; i < limit; i++) {
        assert(hasht_local.contains(i) == true);
        assert(hasht_local.get(i) == static_cast<int>(limit-i));
    }
    for(unsigned int i = (limit - 1); i > 0; i--)
        assert(hasht_local.erase(i) == true);
    assert(hasht_local.erase(0) == true);
        
    for(unsigned int i = 0; i < limit; i++)
        assert(hasht_local.get(i, -1) == -1);
    
    atomic_hashtable::SplitOrderedHash<int,int> *hasht = NULL;

    cout << "Basic functionality check complete, atomic operations are supported.\n";

    

    uint64_t total_time = 0;
    double throughput = 0.0;

    cout << "\n";

    cout << "=========== std::unordered_map Test (Lookups Only) ===========\n";
    unsigned int limit_tmp = limit;
    std::unordered_map<int, int> *stdhash = NULL;
    for(unsigned int size_iter = 1; size_iter <= limit_increase_iter; size_iter++) {
        total_time = 0;
        throughput = 0.0;
        cout << "-----Number of Iterations: " << limit_tmp << "-----\n";
        for(unsigned int j = 0; j < repeat; j++) {
            stdhash = new unordered_map<int, int>();
            for (int i = 0; i < static_cast<int>(limit_tmp) * 10; i++)
                (*stdhash)[i] = static_cast<int>(limit_tmp)-i;
            auto start = chrono::high_resolution_clock::now(); 
            for(int i = 0; i < static_cast<int>(limit_tmp) * 10; i++)
                assert((*stdhash)[i] == static_cast<int>(limit_tmp)-i);
            auto end = chrono::high_resolution_clock::now(); 
            auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
            if (print_time)
                cout << duration.count() << " ms\n";
            total_time += duration.count();
            throughput += static_cast<double>(limit_tmp * 10) / duration.count();
            delete stdhash;
        }
        cout << "Average Time: " << static_cast<double>(total_time)/repeat << "\n";
        cout << "Average Throughput: " << (throughput/repeat) << "/ms\n";
        limit_tmp = limit_tmp * 2;
    }
    
    cout << "\n\n";

    cout << "=========== std::unordered_map Test (Insertion Only) ===========\n";
    limit_tmp = limit;
    stdhash = NULL;
    for(unsigned int size_iter = 1; size_iter <= limit_increase_iter; size_iter++) {
        total_time = 0;
        throughput = 0.0;
        cout << "-----Number of Insertion Iterations: " << limit_tmp << "-----\n";
        for(unsigned int j = 0; j < repeat; j++) {
            stdhash = new unordered_map<int, int>();
            auto start = chrono::high_resolution_clock::now(); 
            for(unsigned int i = 0; i < limit_tmp; i++) {
                int key = rand();
                (*stdhash)[key] = i;
                (*stdhash)[key + limit_tmp] = i;
                (*stdhash)[key + limit_tmp + limit_tmp] = i;
            }
            auto end = chrono::high_resolution_clock::now(); 
            auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
            if (print_time)
                cout << duration.count() << " ms\n";
            total_time += duration.count();
            throughput += static_cast<double>(limit_tmp * 3) / duration.count();
            delete stdhash;
        }
        cout << "Average Time: " << static_cast<double>(total_time)/repeat << "\n";
        cout << "Average Throughput: " << (throughput/repeat) << "/ms\n";
        limit_tmp = limit_tmp * 2;
    }

    cout << "\n\n";

    cout << "=========== Split-Order Hash Table Test (Single Thread, Lookups Only) ===========\n";    
    limit_tmp = limit;
    for(unsigned int size_iter = 1; size_iter <= limit_increase_iter; size_iter++) {
        total_time = 0;
        throughput = 0.0;
        cout << "-----Number of Insertion Iterations: " << limit_tmp << "-----\n";
        for(unsigned int j = 0; j < repeat; j++) {
            hasht = new atomic_hashtable::SplitOrderedHash<int,int>();
            hasht->change_delete_cleanup_threshold(delete_limit);
            thread_job_lookup_init(hasht, limit_tmp);
            auto start = chrono::high_resolution_clock::now(); 
            for (int i = 0; i < static_cast<int>(limit_tmp) * 10; i++)
                assert(hasht->get(i) == static_cast<int>(limit_tmp)-i);
            auto end = chrono::high_resolution_clock::now();
            auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
            if (print_time)
                cout << duration.count() << " ms\n";
            total_time += duration.count();
            throughput += static_cast<double>(limit_tmp * 10) / duration.count();
            delete hasht;
        }
        cout << "Average Time: " << static_cast<double>(total_time)/repeat << "\n";
        cout << "Average Throughput: " << (throughput/repeat) << "/ms\n";
        limit_tmp = limit_tmp * 2;
    }
    
    cout << "\n\n";

    cout << "=========== Split-Order Hash Table Test (Single Thread, Insertion Only) ===========\n";    
    limit_tmp = limit;
    for(unsigned int size_iter = 1; size_iter <= limit_increase_iter; size_iter++) {
        total_time = 0;
        throughput = 0.0;
        cout << "-----Number of Insertion Iterations: " << limit_tmp << "-----\n";
        for(unsigned int j = 0; j < repeat; j++) {
            hasht = new atomic_hashtable::SplitOrderedHash<int,int>();
            hasht->change_delete_cleanup_threshold(delete_limit);
            auto start = chrono::high_resolution_clock::now(); 
            for(unsigned int i = 0; i < limit_tmp; i++) {
                int key = rand();
                hasht->insert(key,i);
                hasht->insert(key+limit_tmp,i);
                hasht->insert(key+limit_tmp+limit_tmp,i);
            }

            auto end = chrono::high_resolution_clock::now(); 
            auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
            if (print_time)
                cout << duration.count() << " ms\n";
            total_time += duration.count();
            throughput += static_cast<double>(limit_tmp * 3) / duration.count();
            delete hasht;
        }
        cout << "Average Time: " << static_cast<double>(total_time)/repeat << "\n";
        cout << "Average Throughput: " << (throughput/repeat) << "/ms\n";
        limit_tmp = limit_tmp * 2;
    }
    
    cout << "\n\n";

    cout << "=========== Multithread Test (Lookups Only) ===========\n";
    for(unsigned int thread_num = 2; thread_num <= max_thread_num; thread_num++) {
        unsigned int this_limit = limit;
        for(unsigned int size_iter = 1; size_iter <= limit_increase_iter; size_iter++) {
            total_time = 0;
            throughput = 0.0;
            cout << "-----Number of Threads: " << thread_num << ", Number of Insertion Iterations: " << this_limit << "-----\n";

            for(unsigned int i = 0; i < repeat; i++)
                run_lookup_test(this_limit, delete_limit, thread_num, print_time, total_time, throughput);

            cout << "Average Time: " << static_cast<double>(total_time)/repeat << "\n";
            cout << "Average Throughput: " << (throughput/repeat) << "/ms per thread\n";
            
            this_limit = this_limit * 2;
        }
        cout << "\n";
    }

    cout << "\n\n";

    cout << "=========== Multithread Test (Insertion Only) ===========\n";
    for(unsigned int thread_num = 2; thread_num <= max_thread_num; thread_num++) {
        unsigned int this_limit = limit;
        for(unsigned int size_iter = 1; size_iter <= limit_increase_iter; size_iter++) {
            total_time = 0;
            throughput = 0.0;
            cout << "-----Number of Threads: " << thread_num << ", Number of Insertion Iterations: " << this_limit << "-----\n";

            for(unsigned int i = 0; i < repeat; i++)
                run_insert_test(this_limit, delete_limit, thread_num, print_time, total_time, throughput);

            cout << "Average Time: " << static_cast<double>(total_time)/repeat << "\n";
            cout << "Average Throughput: " << (throughput/repeat) << "/ms per thread\n";
            
            this_limit = this_limit * 2;
        }
        cout << "\n";
    }

    cout << "\n\n";

    cout << "=========== Multithread Test (Insertion and Deletion) ===========\n";
    for(unsigned int thread_num = 2; thread_num <= max_thread_num; thread_num += 2) {
        unsigned int this_limit = limit;
        for(unsigned int size_iter = 1; size_iter <= limit_increase_iter; size_iter++) {
            total_time = 0;
            throughput = 0.0;
            cout << "-----Number of Threads: " << thread_num << ", Number of Insertion Iterations: " << this_limit << "-----\n";

            for(unsigned int i = 0; i < repeat; i++)
                run_deletion_test(this_limit, delete_limit, thread_num, print_time, total_time, throughput);

            cout << "Average Time: " << static_cast<double>(total_time)/repeat << "\n";
            cout << "Average Throughput: " << (throughput/repeat) << "/ms insertions per thread\n";
            
            this_limit = this_limit * 2;
        }
        cout << "\n";
    }

    cout << "\n\nBenchmark Complete!\n";
}
