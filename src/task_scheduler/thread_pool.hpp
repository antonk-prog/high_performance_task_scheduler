#pragma once

#include "task_scheduler/detail/chase_lev_deque.hpp"
#include "task_scheduler/detail/inbox.hpp"
#include "task_scheduler/detail/task.hpp"

#include <atomic>
#include <cstddef>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace task_scheduler {

class ThreadPool {
public:
    static std::size_t default_thread_count() noexcept;

    explicit ThreadPool(std::size_t thread_count = default_thread_count(),
                        std::size_t queue_capacity = 1024);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    // Допускает одновременные вызовы из внешних потоков и из задач пула.
    // Возвращаемое callable значение игнорируется. Исключения считаются в failed_tasks().
    template <class F>
    void submit(F&& function) {
        using Function = std::decay_t<F>;
        static_assert(std::is_invocable_v<Function&>, "task must be callable without arguments");
        auto task = std::make_unique<detail::CallableTask<Function>>(
            std::forward<F>(function));
        enqueue(std::move(task));
    }

    // Только для внешнего потока. Включает дочерние задачи.
    // Чтобы дождаться конкретного набора задач, сначала завершите его producers.
    void wait_idle() const;

    std::size_t thread_count() const noexcept { return workers_.size(); }
    std::size_t failed_tasks() const noexcept {
        return failed_tasks_.load(std::memory_order_relaxed);
    }

private:
    struct Worker {
        explicit Worker(std::size_t capacity) : local(capacity) {}

        detail::ChaseLevDeque<detail::Task> local;
        detail::Inbox inbox;
        detail::Task* batch = nullptr; // Только для владельца, уже отсоединён от inbox.
    };

    void enqueue(std::unique_ptr<detail::Task> task) noexcept;
    void refill_local(Worker& worker) noexcept;
    void run_worker(std::size_t index) noexcept;
    void execute(detail::Task* task) noexcept;

    static_assert(std::atomic<std::size_t>::is_always_lock_free,
                  "lock-free counters are required");
    static_assert(std::atomic<bool>::is_always_lock_free,
                  "lock-free stop flag is required");

    std::vector<std::unique_ptr<Worker>> workers_;
    std::vector<std::thread> threads_;
    alignas(64) std::atomic<std::size_t> outstanding_{0};
    alignas(64) std::atomic<std::size_t> next_worker_{0};
    alignas(64) std::atomic<std::size_t> failed_tasks_{0};
    alignas(64) std::atomic<bool> stopping_{false};

    static thread_local ThreadPool* current_pool_;
    static thread_local std::size_t current_worker_;
};

} // namespace task_scheduler
