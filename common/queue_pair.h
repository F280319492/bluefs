//
// Created by 费长红 on 2021/10/31.
//

#ifndef BLUEFS_QUEUE_PAIR_H
#define BLUEFS_QUEUE_PAIR_H

#include <boost/lockfree/queue.hpp>
#include <unordered_map>
#include <atomic>
#include <assert.h>

struct queue_qair {
    uint64_t queue_num;
    boost::lockfree::spsc_queue<void*, boost::lockfree::capacity<1024>> queues[2];

    queue_qair(uint64_t num) : queue_num(num) {}
};

struct queue_qairs {
    uint64_t thread_seq;
    std::mutex thread_seq_lock;
    std::unordered_map<int, queue_qair*> queue_qair_hash_map;

    uint64_t register_queue_pair();
    void unregister_queue_pair(uint64_t);
    queue_qair* get_queue_qair(uint64_t);

    queue_qairs() : thread_seq(0) {}
};

extern queue_qairs gobal_queue_qairs;

#endif //BLUEFS_QUEUE_PAIR_H
