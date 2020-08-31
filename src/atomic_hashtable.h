#include <atomic>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>  // For std::unique_lock
#include <bitset>
#include <shared_mutex>
#include <sys/types.h>

namespace atomic_hashtable
{

const size_t SIZE_T_BITLENGTH = sizeof(size_t)*8;
const size_t MAX_HASH = size_t(-1L);

/*
 * Partially Non-blocking Hash Table
 * 
 * Implements a modified version of Recursive Split-Ordered Hash Table outlined in Shalev & Shavit (2006).
 * Based on lock-free linked Harris (2001).
 * All credits go to the aforementioned works.
 * 
 * lookups, inserts, deletions, and resizing are lock free.
 * However, memory-reclamation is not; after a certain number of nodes have been deleted,
 *   the hash table will try to gain the lock in order to safely free memory.
 * 
 * Requires C++17 (std::shared_mutex)
 * 
 * POSSIBLE ISSUES
 * 1) Depends on size_t being unsigned long (for __builtin_clzl)
 * 2) Certain GCC built-in operations are used (atomic operations, __builtin_clzl, etc.)
 *  - therefore your platform needs to support these operations.
 */

/* 
 * From https://stackoverflow.com/questions/746171/efficient-algorithm-for-bit-reversal-from-msb-lsb-to-lsb-msb-in-c
 */
static const unsigned char bit_reverse_table[] = 
{
  0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0, 
  0x08, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8, 
  0x04, 0x84, 0x44, 0xC4, 0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4, 
  0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC, 
  0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2, 0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2, 
  0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA,
  0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6, 
  0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE, 0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE,
  0x01, 0x81, 0x41, 0xC1, 0x21, 0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
  0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9, 
  0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5,
  0x0D, 0x8D, 0x4D, 0xCD, 0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD,
  0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3, 
  0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB, 0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB,
  0x07, 0x87, 0x47, 0xC7, 0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7, 
  0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF
};

/* Platform independent bit reverse
 * Hopefully the compiler will unroll the loop as the number of iterations is static
 */ 
inline size_t reverse(size_t value) {
    size_t result = 0;
    for (size_t i = 1; i <= sizeof(size_t); i++) {
        result |= (size_t(bit_reverse_table[value & 0xFF]) << ((sizeof(size_t) - i) * 8));
        value = value >> 8;
    }
    return result;
}


/* class SplitOrderNode
 * Represents each node of the linked list that holds each key value pair
 * Base class is the dummy node, and the subclass SplitOrderValueNode represents the actual value nodes
 */

template<class K, class V>
class SplitOrderNode {
public:
    std::atomic<SplitOrderNode<K,V>*> next;
    size_t reverse_hash;
    const bool is_value;
    SplitOrderNode(size_t reverse_hash) : next(NULL), reverse_hash(reverse_hash), is_value(false) { };
    SplitOrderNode(size_t reverse_hash, SplitOrderNode<K,V>* next) :
        next(next), reverse_hash(reverse_hash), is_value(false) { };
    bool is_lock_free() {
        return next.is_lock_free();
    }

    friend std::ostream& operator<< (std::ostream &out, const SplitOrderNode<K,V> &node) {
        std::bitset<sizeof(size_t) * 8> x(node.reverse_hash);
        out << "NODE_REVHASH:" << x << "_VALUE:" << reverse(node.reverse_hash) << "_ISVALUE:" << node.is_value;
        if(is_logically_deleted(node.next.load()))
            out << "_DELETED";
        return out;
    }
protected:
    SplitOrderNode(size_t reverse_hash, SplitOrderNode<K,V>* next, bool is_value) :
        next(next), reverse_hash(reverse_hash), is_value(is_value) { };
}; 

template<class K, class V>
class SplitOrderValueNode : public SplitOrderNode<K,V> {
public:
    const K key;
    V value;

    SplitOrderValueNode(K key, V value, SplitOrderNode<K,V> *next, size_t rev_hash)
    : SplitOrderNode<K,V>(rev_hash, next, true), key(key), value(value) { };
    SplitOrderValueNode(K key, V value, SplitOrderNode<K,V> *next)
    : SplitOrderNode<K,V>(0, next, true), key(key), value(value) {
            std::hash<K> hasher;
            this->hash_value = reverse(hasher(key));
    };
}; 


template<class K, class V>
class LockFreeDeleteQueueNode {
public:
    SplitOrderNode<K,V> *item;
    size_t hash;
    std::atomic<LockFreeDeleteQueueNode<K,V>*> next;

    LockFreeDeleteQueueNode(SplitOrderNode<K,V> *item)
    : item(item), hash(0), next(NULL) { }
    LockFreeDeleteQueueNode(SplitOrderNode<K,V> *item, size_t hash)
    : item(item), hash(hash), next(NULL) { }
    LockFreeDeleteQueueNode(SplitOrderNode<K,V> *item, size_t hash, LockFreeDeleteQueueNode<K,V> *next)
    : item(item), hash(hash), next(next) { }
};

/* Lock-free pushing queue to keep track of delete nodes */
template<class K, class V>
class LockFreeDeleteQueue {
private:
LockFreeDeleteQueueNode<K,V> *head;
std::atomic<LockFreeDeleteQueueNode<K,V>*> tail;
public:
    LockFreeDeleteQueue() {
        LockFreeDeleteQueueNode<K,V> *dummy = new LockFreeDeleteQueueNode<K,V>(NULL, 0, NULL);
        this->head = dummy;
        this->tail.store(dummy);
    }
    ~LockFreeDeleteQueue() {
        LockFreeDeleteQueueNode<K,V> *item = head;
        LockFreeDeleteQueueNode<K,V> *to_delete;
        while (item) {
            to_delete = item;
            item = item->next;
            delete to_delete;
        }
    }

    void push(SplitOrderNode<K,V>* item, size_t hash) {
        LockFreeDeleteQueueNode<K,V> *new_node = new LockFreeDeleteQueueNode<K,V>(item, hash);
        LockFreeDeleteQueueNode<K,V> *null_ptr = NULL;
        LockFreeDeleteQueueNode<K,V> *curr_tail = tail.load();
        while(!((curr_tail->next).compare_exchange_weak(null_ptr, new_node))) {
            null_ptr = NULL;
            curr_tail = tail.load();
        }
        tail.store(new_node);
    }

    /* Pop is not thread safe as we will call it only in a single-threaded setting */
    SplitOrderNode<K,V>* pop(size_t *hash) {
        LockFreeDeleteQueueNode<K,V> *node = this->head->next;
        if (node) {
            this->head->next.store(node->next);
            SplitOrderNode<K,V>* item = node->item;
            *hash = node->hash;
            delete node;
            return item;
        }
        else {
            this->tail = this->head;
            return NULL;
        }
    }
};


/* Helper functions for bit-marking pointers to distinguish deleted nodes */

template<class K, class V>
inline bool is_logically_deleted(SplitOrderNode<K,V> *next_pointer) {
    return (reinterpret_cast<uintptr_t>(next_pointer) & 0x1UL) != 0UL;
}

template<class K, class V>
inline SplitOrderNode<K,V>* get_logically_deleted_ptr(SplitOrderNode<K,V> *next_pointer) {
    return reinterpret_cast<SplitOrderNode<K,V>*>(reinterpret_cast<uintptr_t>(next_pointer) | 0x1UL);
}

template<class K, class V>
inline SplitOrderNode<K,V>* get_unmarked_ptr(SplitOrderNode<K, V> *next_pointer) {
    return reinterpret_cast<SplitOrderNode<K,V>*>(reinterpret_cast<uintptr_t>(next_pointer) & (~0x1UL));
}

/* Helper functions to get index for shortcuts */

inline size_t get_levelone_index(size_t mod_hash) {
    return mod_hash < 2UL ? 0UL : (SIZE_T_BITLENGTH - 1UL) - (__builtin_clzl(mod_hash));
}

inline size_t get_leveltwo_index(size_t mod_hash, size_t levelone_i) {
    return mod_hash ^ (1UL << levelone_i); // For LEVEL1[0] the results are flipped for index 0 and 1!
}

/* SplitOrderHash */

template<class K, class V>
class SplitOrderedHash {
private:
    SplitOrderNode<K,V>** bucket_pointers[SIZE_T_BITLENGTH]; // Two-level indirection for shortcut nodes
    std::hash<K> hasher;
    std::atomic<size_t> mask; // num_buckets = mask + 1;
    // Resize policy (average bucket load)
    std::atomic<double> max_bucket_load = 2.5;
    // Memory reclamation initialization policy
    uint_fast64_t max_deleted = 500;
    uint_fast64_t min_deleted = 10;
    // Entry count & deletion count
    std::atomic_uint_fast64_t num_entries;
    std::atomic_uint_fast64_t num_deleted;
    // Deletion & memory reclamation tracking
    LockFreeDeleteQueue<K,V>* to_delete;
    mutable std::shared_mutex delete_lock;

    inline size_t get_hash_value(const K& key) {
        size_t hash_value = this->hasher(key);
        return (hash_value == MAX_HASH ? 0 : hash_value);
    }

    /* Memory reclamation
     * Runs after getting unique lock, so is single-threaded
     */
    void clean_deleted() {
        if (this->num_deleted < this->min_deleted)
            return;
        
        size_t hash_val;
        for(SplitOrderNode<K,V>* item = to_delete->pop(&hash_val); item != NULL; item = to_delete->pop(&hash_val)) {
            size_t reversed = reverse(hash_val);
            size_t levelone_i, leveltwo_i;
            SplitOrderNode<K,V>* shortcut;

            size_t mod_hash;
            if (item->is_value)
                mod_hash = hash_val & mask;
            else // For deleted shortcut nodes, we need access from parent
                mod_hash = get_leveltwo_index(hash_val, get_levelone_index(hash_val));
            // Get valid shortcut
            while(true) {
                levelone_i = get_levelone_index(mod_hash);
                leveltwo_i = get_leveltwo_index(mod_hash, levelone_i);
                shortcut = this->bucket_pointers[levelone_i][leveltwo_i];
                if (shortcut != NULL && (shortcut->reverse_hash <= item->reverse_hash))
                    break;
                mod_hash = leveltwo_i;
            }

            SplitOrderNode<K,V>* this_left = shortcut;
            SplitOrderNode<K,V>* this_right = shortcut->next;
            while(true) {
                if(this_right->reverse_hash > reversed) {
                    this_right = item;
                    delete this_right;
                    break;
                }
                else if (this_right == item) {
                    // Found
                    if (is_logically_deleted((this_left->next).load()))
                        this_left->next = get_logically_deleted_ptr((this_right->next).load());
                    else
                        this_left->next = get_unmarked_ptr((this_right->next).load());
                    delete this_right;
                    break;
                }
                else {
                    this_left = this_right;
                    this_right = get_unmarked_ptr((this_right->next).load());
                }
            }
        }
        this->num_deleted = 0;
        return;
    }
    
    inline void finish_insert() {
        this->num_entries++;
        if ((this->num_entries / double(mask + 1)) > max_bucket_load)
            resize();
    }
    
    /* Non-blocking resize() */
    void resize() {
        size_t current_bucket = mask + 1;
        SplitOrderNode<K,V>** new_level = new SplitOrderNode<K,V>*[current_bucket];
        std::memset(new_level, 0, sizeof(SplitOrderNode<K,V>*) * current_bucket);

        SplitOrderNode<K,V>** null_ptr = NULL;
        // Ensure this level isn't already initialized
        if (__atomic_compare_exchange_n(&(this->bucket_pointers[get_levelone_index(current_bucket)]), &null_ptr, new_level, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
            this->mask.fetch_or(current_bucket); // Update mask as new level is inserted
        }
        else {
            delete[] new_level;
        }
    }

    /* Internal method to recursively initialize shortcut nodes
     * Parent's hash value is equal to index of level 2 of bucket pointers
     */
    SplitOrderNode<K,V>* get_shortcut(size_t mod_hash) {
        size_t levelone_i = get_levelone_index(mod_hash);
        size_t leveltwo_i = get_leveltwo_index(mod_hash, levelone_i);

        SplitOrderNode<K,V> *shortcut = this->bucket_pointers[levelone_i][leveltwo_i];
        if (shortcut == NULL) {
            size_t reversed = reverse(mod_hash);
            shortcut = new SplitOrderNode<K,V>(reversed);

            // Find place and insert shortcut node into the linked list
            SplitOrderNode<K,V> *parent_shortcut = get_shortcut(leveltwo_i);
            SplitOrderNode<K,V> *this_left, *this_right;
            while(true) {
                SplitOrderNode<K,V> *this_right_next;
                SplitOrderNode<K,V> *right_of_last_undeleted_left = parent_shortcut->next;
                this_left = parent_shortcut;
                this_right = parent_shortcut->next;

                // Main finding loop
                while(true) {
                    this_right_next = this_right->next;
                    if (is_logically_deleted(this_right_next)) {
                        this_right = get_unmarked_ptr(this_right_next);
                    }
                    else {
                        if (this_right->reverse_hash == reversed) {
                            if (!(this_right->is_value) &&
                                this->bucket_pointers[levelone_i][leveltwo_i] != NULL) {
                                delete shortcut;
                                return this->bucket_pointers[levelone_i][leveltwo_i];
                            }
                            else
                                break;
                        }
                        else if (this_right->reverse_hash > reversed)
                            break;
                        this_left = this_right;
                        right_of_last_undeleted_left = this_right_next;
                        this_right = right_of_last_undeleted_left;
                    }
                }

                // Attempt to remove in-between deleted nodes if left and right not adjacent
                if (right_of_last_undeleted_left != this_right) {
                    // If no node has been added after the last undeleted left, attempt to remove deleted node from main linked list
                    // Core principle is to never let "left" be a deleted node
                    if (!(this_left->next.compare_exchange_strong(right_of_last_undeleted_left, this_right))) {
                        continue;
                    }
                }
                
                // Try inserting shortcut node in the list
                shortcut->next = this_right;
                if (this_left->next.compare_exchange_strong(this_right, shortcut)) {
                    break;
                }
            }

            // Insertion successful, have to use atomic CAS on bucket_pointers
            SplitOrderNode<K,V> *null_node = NULL;
            if (!__atomic_compare_exchange_n(&(this->bucket_pointers[levelone_i][leveltwo_i]), &null_node, shortcut, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
                // Some other thread created a different shortcut, so attempt to mark as deleted
                SplitOrderNode<K,V> *shortcut_next = shortcut->next;
                while(!(shortcut->next.compare_exchange_strong(shortcut_next, get_logically_deleted_ptr(shortcut_next)))) {
                    //shortcut_next = shortcut->next; this is done by the CAS operation
                }
                // Add shortcut_node to delete queue
                this->to_delete->push(shortcut, mod_hash);
                this->num_deleted++;

                shortcut = this->bucket_pointers[levelone_i][leveltwo_i];
            }
        }
        return shortcut;
    }

    /* Finds node with given key & hash
     * Receives left and right node** as output parameters
     * Returns true if found key is exact match, false if not
     * Function receives reversed value to save computation time (caller can reuse reversed value)
     */
    bool find_in_list(const K& key, size_t target_hash, size_t reversed, SplitOrderNode<K,V>** left, SplitOrderNode<K,V>** right) {
        while(true) {
            size_t mod_hash = target_hash & mask;
            SplitOrderNode<K,V>* shortcut = this->get_shortcut(mod_hash);
            SplitOrderNode<K,V> *right_of_last_undeleted_left, *this_left, *this_right, *this_right_next;
            bool match = false;

            right_of_last_undeleted_left = shortcut->next;
            this_left = shortcut;
            this_right = shortcut->next;
            // Main finding loop
            while(true) {
                this_right_next = this_right->next;
                if (is_logically_deleted(this_right_next)) {
                    this_right = get_unmarked_ptr(this_right_next);
                }
                else {
                    if (this_right->reverse_hash > reversed)
                        break;
                    else if (this_right->reverse_hash == reversed && 
                                this_right->is_value &&
                                (static_cast<SplitOrderValueNode<K,V> *>(this_right))->key == key) {
                        // This separate logic for searching equal hash values is necessary
                        // As shortcut nodes needs to be inserted before equal values
                        // where are value nodes needs to be searched until all equal values
                        match = true;
                        break;
                    }
                    this_left = this_right;
                    right_of_last_undeleted_left = this_right_next; // This is okay because this_right_next is not marked
                    this_right = right_of_last_undeleted_left;
                }
            }
            (*left) = this_left;
            (*right) = this_right;

            // Attempt to remove in-between deleted nodes if left and right not adjacent
            if (right_of_last_undeleted_left != this_right) {
                // If no node has been added after the last undeleted left, attempt to remove deleted node from main linked list
                if (this_left->next.compare_exchange_strong(right_of_last_undeleted_left, this_right))
                    return match;
                else
                    continue;
            }
            return match;
        }
    }

public:
     /* Constructor - constructs the hash table with given capacity
      * Capacity will be rounded up to the nearest power of 2, with a minimum of 4
      */
    SplitOrderedHash(size_t capacity = 4) {
        // Configure size to be power of 2, at least 4
        for(size_t i = 0; i < SIZE_T_BITLENGTH; i++) {
            bucket_pointers[i] = NULL;
        }
        capacity = capacity < 4 ? 3 : capacity - 1;
        size_t max_levelone = (SIZE_T_BITLENGTH - (__builtin_clzl(capacity)));
        bucket_pointers[0] = new SplitOrderNode<K,V>*[2];
        bucket_pointers[1] = new SplitOrderNode<K,V>*[2];
        // Initialize shortcut nodes for 0~3
        // Due to the way we get second level index, [0][0] corresponds to 1, and [0][1] to 0
        SplitOrderNode<K,V>* tail_sentinel = new SplitOrderNode<K,V>(MAX_HASH);
        bucket_pointers[1][1] = new SplitOrderNode<K,V>(reverse(3UL), tail_sentinel);
        bucket_pointers[0][0] = new SplitOrderNode<K,V>(reverse(1UL), bucket_pointers[1][1]);
        bucket_pointers[1][0] = new SplitOrderNode<K,V>(reverse(2UL), bucket_pointers[0][0]);
        bucket_pointers[0][1] = new SplitOrderNode<K,V>(0UL, bucket_pointers[1][0]);

        // Initialize second level shortcut array
        for(size_t i = 2; i < max_levelone; i++) {
            size_t bucket_size = 1UL << i;
            SplitOrderNode<K,V>** new_level = new SplitOrderNode<K,V>*[bucket_size];
            std::memset(new_level, 0, sizeof(SplitOrderNode<K,V>*) * bucket_size);
            bucket_pointers[i] = new_level;
        }
            
        
        hasher = std::hash<K>();
        mask = (1UL << max_levelone) - 1UL; // num_buckets = mask + 1;

        num_entries = 0;
        num_deleted = 0;
        to_delete = new LockFreeDeleteQueue<K,V>();
    }
    
    /* Checks if this hash table should be used at all
     * Not locking is only meaningful if lock-free atomics are supported
     */
    bool is_lock_free() {
        return mask.is_lock_free() && bucket_pointers[0][0]->is_lock_free()
                && max_bucket_load.is_lock_free();
    }

    /* Change max average bucket load before increasing capacity */
    void change_resize_threshold(double t) {
        t = t < 1.0 ? 1.0 : t;
        this->max_bucket_load = t;
    }

    /* Change maximum number of deleted nodes before initiating memory reclamation
     * If more nodes than the threshold are deleted, the cleanup will proceed
     */
    void change_delete_cleanup_threshold(uint64_t new_max_deleted) {
        new_max_deleted = new_max_deleted < 1UL ? 1UL : new_max_deleted;
        this->max_deleted = new_max_deleted;
    }

    /* Change minimum number of deleted nodes before initiating memory reclamation
     * If less nodes than the minimum are deleted, the cleanup will not proceed
     * 
     * This is helpful for optimization; if the deleted count is greater than the threshold,
     * it is likely multiple threads are trying to initiate deleting.
     * That means clean_deleted() will run even when all nodes have been deleted, and we traverse the
     * linked list without achieving anything.
     */
    void change_minimum_delete_cleanup_threshold(uint64_t new_min_deleted) {
        if (new_min_deleted < this->max_deleted)
            this->min_deleted = new_min_deleted;
    }

    /* Returns number of entries */
    uint_fast64_t size() {
        return this->num_entries.load();
    }

    uint_fast64_t deleted_size() {
        return this->num_deleted.load();
    }

    /* Get value from given key, with default value to return if value not found */
    V get(const K& key, const V& default_value) {
        SplitOrderNode<K,V> *right, *left;
        size_t hash_value = get_hash_value(key);
        size_t reversed = reverse(hash_value);

        if (this->num_deleted > this->max_deleted) {
            std::unique_lock<std::shared_mutex> lock(this->delete_lock);
            clean_deleted();
            lock.unlock();
        }
        std::shared_lock<std::shared_mutex> slock(this->delete_lock);


        bool match = find_in_list(key, hash_value, reversed, &left, &right);
        if (!match) {
            return default_value;
        }
        else {
            // Assume below cast is safe, as we found a match
            V ret_value = static_cast<SplitOrderValueNode<K,V> *>(right)->value;
            return ret_value;
        }
    }

    /* Get value from given key
     * If entry with given key not found, throws a std::out_of_range error
     */
    V get(const K& key) {
        SplitOrderNode<K,V> *right, *left;
        size_t hash_value = get_hash_value(key);
        size_t reversed = reverse(hash_value);
        
        if (this->num_deleted > this->max_deleted) {
            std::unique_lock<std::shared_mutex> lock(this->delete_lock);
            clean_deleted();
            lock.unlock();
        }
        std::shared_lock<std::shared_mutex> slock(this->delete_lock);


        bool match = find_in_list(key, hash_value, reversed, &left, &right);
        if (!match)
            throw std::out_of_range("Entry with given key not found");
        else {
            // Assume below cast is safe, as we found a match
            V ret_value = static_cast<SplitOrderValueNode<K,V> *>(right)->value;
            return ret_value;
        }
    }

    /* Checks if entry with given key exists */
    bool contains(const K& key) {
        SplitOrderNode<K,V> *right, *left;
        size_t hash_value = get_hash_value(key);
        size_t reversed = reverse(hash_value);
        
        if (this->num_deleted > this->max_deleted) {
            std::unique_lock<std::shared_mutex> lock(this->delete_lock);
            clean_deleted();
            lock.unlock();
        }
        std::shared_lock<std::shared_mutex> slock(this->delete_lock);

        bool match = find_in_list(key, hash_value, reversed, &left, &right);
        return match;

    }

    /* Inserts given key, value pair */
    void insert(const K& key, V value) {
        SplitOrderNode<K,V> *right, *left, *new_node;
        size_t hash_value = get_hash_value(key);
        size_t reversed = reverse(hash_value);
        
        if (this->num_deleted > this->max_deleted) {
            std::unique_lock<std::shared_mutex> lock(this->delete_lock);
            clean_deleted();
            lock.unlock();
        }
        std::shared_lock<std::shared_mutex> slock(this->delete_lock);


        // First iteration - separated in order to reduce unnecessary allocations
        bool match = find_in_list(key, hash_value, reversed, &left, &right);
        if (!match) {
            new_node = new SplitOrderValueNode(key, value, right, reversed);
            if(left->next.compare_exchange_strong(right, new_node)) {
                slock.unlock();
                return finish_insert();
            }
        }
        else {
            static_cast<SplitOrderValueNode<K,V> *>(right)->value = value;
            return;
        }

        // First iteration CAS failed, therefore run loop
        while(true) {
            match = find_in_list(key, hash_value, reversed, &left, &right);
            if (!match) {
                new_node->next = right;
                if(left->next.compare_exchange_weak(right, new_node)) {
                    slock.unlock();
                    return finish_insert();
                }
            }
            else {
                delete new_node;
                static_cast<SplitOrderValueNode<K,V> *>(right)->value = value;
                return;
            }
        }
    }

    /* Attempts to delete entry with given key
     * Returns true if something is deleted, false otherwise (std::unordered_map returns number of items deleted (so 0 or 1))
     */
    bool erase(const K& key) {
        SplitOrderNode<K,V> *right, *left, *right_next;
        size_t hash_value = get_hash_value(key);
        size_t reversed = reverse(hash_value);

        if (this->num_deleted > this->max_deleted) {
            std::unique_lock<std::shared_mutex> lock(this->delete_lock);
            clean_deleted();
            lock.unlock();
        }
        std::shared_lock<std::shared_mutex> slock(this->delete_lock);

        while(true) {
            if (find_in_list(key, hash_value, reversed, &left, &right)) {
                right_next = right->next;
                if (is_logically_deleted(right_next)) {
                    return false;
                }
                else {
                    // Logically delete if 1) it was not deleted before and 2) right->next didn't change
                    // We can't delete if right->next changed, as the changed value after CAS wouldn't reflect the changed pointer
                    if (right->next.compare_exchange_strong(right_next, get_logically_deleted_ptr(right_next))) {
                        // Add to deletion queue and increment counter
                        this->to_delete->push(right, hash_value);
                        slock.unlock();
                        this->num_deleted++;
                        this->num_entries--;
                        return true;
                    }
                }
            }
            else {
                return false;
            }
        }
    }

    /* Prints all nodes in order stored in the internal linked list */
    void print_node() {
        if (this->num_deleted > this->max_deleted) {
            std::unique_lock<std::shared_mutex> lock(this->delete_lock);
            clean_deleted();
            lock.unlock();
        }
        std::shared_lock<std::shared_mutex> slock(this->delete_lock);

        SplitOrderNode<K,V> *item = this->bucket_pointers[0][1];
        while(item != NULL) {
            std::cout << item << "_" << (*item) << "\n";
            item = get_unmarked_ptr(item->next.load());
        }
        std::cout << "\n";
    }

    /* Destructor - NOT THREAD SAFE! */
    ~SplitOrderedHash() {
        clean_deleted();
        delete this->to_delete;
        SplitOrderNode<K,V> *item, *prev;
        item = this->bucket_pointers[0][1];
        while(item != NULL) {
            prev = item;
            item = get_unmarked_ptr(item->next.load());
            delete prev;
        }
        
        for(size_t i = 0; i < SIZE_T_BITLENGTH; i++) {
            if (bucket_pointers[i] == NULL)
                break;
            delete[] bucket_pointers[i];
        }
    }
};

}
