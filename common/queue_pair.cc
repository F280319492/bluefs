#include "queue_pair.h"
#include <iostream>


int __attribute__((noinline)) queue_qairs::register_queue_pair()
{
    int queue_num = -1;
    {
        std::unique_lock<std::mutex> l(queue_seq_lock);
        queue_num = queue_seq++;
        queue_qair* q = new queue_qair(queue_num);
        assert(queue_qair_hash_map.count(queue_num) == 0);
        queue_qair_hash_map[queue_num] = q;
        dev_queues[queue_num%shard_num].push_back(q);
        local_queue = q;
    }
    return queue_num;
}

void queue_qairs::unregister_queue_pair(int queue_num)
{
    assert(queue_qair_hash_map.count(queue_num) == 1);
    queue_qair_hash_map.erase(queue_num);
    local_queue = nullptr;
}

queue_qair* queue_qairs::get_queue_qair(int queue_num)
{
    if (queue_qair_hash_map.count(queue_num))
        return queue_qair_hash_map[queue_num];
    else
	return nullptr;
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

void queue_qairs::push(int queue_id, void* val, int idx)
{
    queue_qair_hash_map[queue_id]->push(val, idx);
}

bool queue_qairs::pop(int queue_id, void*& val, int idx)
{
    return queue_qair_hash_map[queue_id]->pop(val, idx);
}

queue_qairs gobal_queue_qairs;
thread_local int local_queue_id = -1;
thread_local queue_qair* local_queue = nullptr;
