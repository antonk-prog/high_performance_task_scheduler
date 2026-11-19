#pragma once

#include "task_scheduler/detail/task.hpp"

#include <atomic>

namespace task_scheduler {
namespace detail {

// Несколько отправителей и потребителей: exchange отдаёт список ровно одному.
// Отправитель не разыменовывает head: hazard pointers здесь не нужны.
class Inbox {
public:
    void push(Task* task) noexcept {
        push_list(task, task);
    }

    // first..last принадлежат вызывающему. После CAS их нельзя разыменовывать.
    void push_list(Task* first, Task* last) noexcept {
        first->inbox_tail = last;
        // Старую голову не разыменовываем; acquire для неё не требуется.
        auto* head = head_.load(std::memory_order_relaxed);
        do {
            last->next = head;
        } while (!head_.compare_exchange_weak(head, first,
                                            // Публикуем next и содержимое всех узлов.
                                            std::memory_order_release,
                                            // При неудаче нужен только новый адрес.
                                            std::memory_order_relaxed));
    }

    Task* take_all(Task** tail = nullptr) noexcept {
        auto* list = head_.exchange(nullptr, std::memory_order_acquire);
        // Кэш хвоста исключает повторный O(N) обход возвращённого остатка.
        if (tail != nullptr) {
            *tail = nullptr;
            auto* segment = list;
            while (segment != nullptr) {
                *tail = segment->inbox_tail;
                segment = (*tail)->next;
            }
        }
        return list;
    }

private:
    alignas(64) std::atomic<Task*> head_{nullptr};
};

} // namespace detail
} // namespace task_scheduler
