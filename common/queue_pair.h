//
// Created by 费长红 on 2021/10/31.
//

#ifndef BLUEFS_QUEUE_PAIR_H
#define BLUEFS_QUEUE_PAIR_H

#include <boost/lockfree/spsc_queue.hpp>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <vector>
#include <assert.h>

struct queue_qair {
    uint64_t queue_num;
    boost::lockfree::spsc_queue<void*, boost::lockfree::capacity<1024>> queues[2];

    queue_qair(uint64_t num) : queue_num(num) {}
};

struct queue_qairs {
    int shard_num;
    int queue_seq;
    std::mutex queue_seq_lock;
    std::unordered_map<int, queue_qair*> queue_qair_hash_map;
    std::vector<std::vector<queue_qair*>> dev_queues;

    int register_queue_pair();
    void unregister_queue_pair(int);
    queue_qair* get_queue_qair(int);
    std::vector<queue_qair*>& get_dev_queue(int idx);
    void Init(int num);
    void push(int queue_id, void* val, int idx);
    void pop(int queue_id, void* val, int idx);

    queue_qairs() : shard_num(0), thread_seq(0) {}
};

extern queue_qairs gobal_queue_qairs;

#endif //BLUEFS_QUEUE_PAIR_H
