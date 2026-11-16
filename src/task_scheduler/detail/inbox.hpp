#pragma once

#include "task_scheduler/detail/task.hpp"

#include <atomic>

namespace task_scheduler {
namespace detail {

// MPSC: несколько отправителей, только worker-владелец забирает batch.
// Отправитель не разыменовывает head: hazard pointers здесь не нужны.
class Inbox {
public:
    void push(Task* task) noexcept {
        auto* head = head_.load(std::memory_order_relaxed);
        do {
            task->next = head;
        } while (!head_.compare_exchange_weak(head, task,
                                            std::memory_order_release,
                                            std::memory_order_relaxed));
    }

    Task* take_all() noexcept {
        auto* list = head_.exchange(nullptr, std::memory_order_acquire);
        // После exchange список принадлежит только потребителю.
        // Разворачиваем его, чтобы старые задачи попадали в deque первыми.
        Task* reversed = nullptr;
        while (list != nullptr) {
            auto* next = list->next;
            list->next = reversed;
            reversed = list;
            list = next;
        }
        return reversed;
    }

private:
    alignas(64) std::atomic<Task*> head_{nullptr};
};

} // namespace detail
} // namespace task_scheduler
