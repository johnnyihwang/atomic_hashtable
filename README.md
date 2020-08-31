# Partially Non-Blocking Hash Table

This implements a modified version of Recursive Split-Ordered Hash Table outlined in Shalev & Shavit (2006) in C++, with support for memory reclamation. The internal representation is a lock-free linked list outlined in Harris (2001). The hash table enables concurrent lookups, insertions, and deletions.

All credits go to the aforementioned works.

## Requirements
* GCC/C++17 (`std::shared_mutex` used)
* 64-bit platform (`size_t` should be equal to `unsigned long`)
* Architecture support for atomic `long`/`double` and atomic CAS operations
    * Check for support for your platform using `is_lock_free()` (See example below)

Tested on x86-64 Linux.

## What do you mean by "Partially Non-Blocking"?

Yes, that is a contradictory statement - this data structure is *not a non-blocking data structure* as the failure of one thread may cause other threads to fail/suspend. This is because at the beginning of all operations we acquire a `shared_lock`, and if the number of deleted entries exceed a certain threshold, we acquire a `unique_lock` to free the deleted entries.

In other words, because all lookups/insertions/deletions proceed under a `shared_lock`, they can all be executed concurrently. Moreover, if deletions are not used, then because we never have to obtain an exclusive lock, the data structure is indeed non-blocking (in fact, lock-free)! It is *only when we reclame memory* for deleted entries that we require a mutex - in any other case, all operations are executed concurrently without locks.

This implementation was done because the lock-free linked list outlined in Harris (2001) relies on garbage collection in order to reclame memory for deleted nodes. This is not present in C++. In order to work around this, we gain an exclusive lock when freeing memory to ensure that we are not freeing nodes being accessed by other threads.

The memory reclamation threshold can be adjusted so that in concurrent sections `unique_lock` would never be required.

To sum up, the following operations can occur concurrently without blocking:

* Lookup
* Insertion
* Deletion
* Resizing

The following operation cannot be executed concurrently (i.e. requires mutex)

* Memory reclamation

## Example

    #include <cassert>
    #include "atomic_hashtable.h"

    atomic_hashtable::SplitOrderedHash<int,int> hasht(10);
    std::cout << hasht.is_lock_free() << "\n"; // Checks if atomic operations are supported

    /* Insertions */
    for(int i = 0; i < 100; i++)
        hasht.insert(i,100-i); // Inserts value 100-i at key i

    /* Lookups/finds */
    for(int i = 0; i < 100; i++) {
        assert(hasht.contains(i) == true);
        assert(hasht.get(i) == 100-i); // Gets the value stored for key i, throws out_of_range error if not found
    }

    /* Deletions */
    for(int i = 99; i >= 0; i--)
        assert(hasht.erase(i) == true); // Deletes entry at key i; returns true if a node is deleted, false otherwise

    for(int i = 0; i < 100; i++)
        assert(hasht.get(i, -1) == -1); // Gets the value stored for key i, returns given value if not found
    
    /* Resizing & memory freeing policy changes */
    hasht.change_resize_threshold(1.5);
    hasht.change_delete_cleanup_threshold(500);



## Benchmarking
An example benchmark program has been included. Compile, then run with `hash_bench`.

One can change the number of iterations, number of threads, etc. using arguments. Run `hash_bench --help` for more information.

Single-threaded performance for finds should be around 40%, and insertions should be around 70% of `std::unordered_map` regardless of hash table size.

## Implementation Details

### Concurrent Resizing
This hash table supports resizing (increasing number of buckets). The resizing policy is based on average bucket load - if the successful insertion of a key-value pair causes the average bucket load to be greater than the resizing threshold, then the number of buckets will be doubled. The average bucket load threshold for resizing can be modified through `change_resize_threshold(double t)`.

The recursive split-ordered hash table in Shalev & Shavit (2006) keeps an array/vector of buckets that doubles in size at each resizing. In order to achieve this without blocking, instead of having a single array that needs to be resized, we use two-level indirection. This allows us to maintain fixed size arrays without needing to resizing them in a concurrent setting.

The first level of array is initialized to length 64. If there are n = 2<sup>m</sup> buckets, then indexes 0 to m-1 are initialized. Each index i of the first level array points to a second level array of size 2<sup>i</sup> (except for index 0, which points to an array of size 2). It is this second level array that holds pointers to each of the buckets. With such scheme, when we initialize a new second level array, we can double the number of buckets.

When we need to resize the hash table (i.e. double the number of buckets), the second level arrays are initialized, then CASed into the first level array. This prevents data races, specifically overlapping allocations by multiple threads which may lead to memory leaks.

### Memory Reclamation
The lock-free linked list outlined in Harris (2001), which this hash table uses in its implementation, uses a 2 stage process for deletion. Nodes get logically deleted first (by being marked as deleted), then physically deleted (by being removed from the linked list). Memory reclamation is relied on using a garbage collector.

In this design, we cannot guarantee that no other threads are accessing nodes which have been removed from the main linked list. To safely free memory without a garbage collector, we obtain a mutex after a certain number of entries have been deleted.

In order to allow fast searching of deleted nodes, we keep a separate stack which keeps track of deleted nodes. This stack is implemented as a linked list supporting lock-free pushes. Everytime a node is logically deleted, it is pushed into this stack. Popping elements is only done in a single-threaded setting.
