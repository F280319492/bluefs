#include "queue_pair.h"

uint64_t queue_qairs::register_queue_pair()
{
    uint64_t thread_num = atomic_inc_return(&thread_seq);
    queue_qair* q = new queue_qair(thread_num);
    assert(queue_qair_hash_map.count(thread_num) == 0);
    queue_qair_hash_map[thread_num] = q;
    return thread_num;
}

void queue_qairs::unregister_queue_pair(uint64_t thread_num)
{
    aassert(queue_qair_hash_map.count(thread_num) == 1);
    queue_qair_hash_map.erase(thread_num);
}

queue_qair* queue_qairs::get_queue_qair(uint64_t thread_num)
{
    aassert(queue_qair_hash_map.count(thread_num) == 1);
    return queue_qair_hash_map[thread_num];
}

queue_qairs gobal_queue_qairs;