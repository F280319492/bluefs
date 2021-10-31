#include "queue_pair.h"

uint64_t queue_qairs::register_queue_pair()
{
    std::unique_lock<std::mutex> l(thread_seq_lock);
    uint64_t thread_num = thread_seq++;
    queue_qair* q = new queue_qair(thread_num);
    assert(queue_qair_hash_map.count(thread_num) == 0);
    queue_qair_hash_map[thread_num] = q;
    dev_queues[thread_num].push_back(q);
    return thread_num;
}

void queue_qairs::unregister_queue_pair(uint64_t thread_num)
{
    assert(queue_qair_hash_map.count(thread_num) == 1);
    queue_qair_hash_map.erase(thread_num);
}

queue_qair* queue_qairs::get_queue_qair(uint64_t thread_num)
{
    assert(queue_qair_hash_map.count(thread_num) == 1);
    return queue_qair_hash_map[thread_num];
}

std::vector<queue_qair*>& queue_qairs::get_dev_queue(int idx)
{
    assert(idx >=0  && idx < shard_num);
    return dev_queues[idx];
}

void queue_qairs::Init(int num)
{
    assert(shard_num == 0);
    shard_num = num;
    dev_queues.resize(num);
    for (int i = 0; i < num; i++) {
        dev_queues[i].clear();
    }
}

queue_qairs gobal_queue_qairs;