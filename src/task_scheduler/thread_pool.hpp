#pragma once

#include "task_scheduler/detail/chase_lev_deque.hpp"
#include "task_scheduler/detail/inbox.hpp"
#include "task_scheduler/detail/task.hpp"

#include <atomic>
#include <cstddef>
#include <condition_variable>
#include <future>
#include <functional>
#include <memory>
#include <mutex>
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
    template <class F>
    auto submit(F&& function) -> std::future<std::invoke_result_t<std::decay_t<F>&>> {
        using Function = std::decay_t<F>;
        using Result = std::invoke_result_t<Function&>;
        static_assert(std::is_invocable_v<Function&>, "task must be callable without arguments");
        std::packaged_task<Result()> packaged(
            [this, fn = Function(std::forward<F>(function))]() mutable -> Result {
                try {
                    return std::invoke(fn);
                } catch (...) {
                    failed_tasks_.fetch_add(1, std::memory_order_relaxed);
                    throw;
                }
            });
        auto result = packaged.get_future();
        auto task = std::make_unique<detail::CallableTask<decltype(packaged)>>(std::move(packaged));
        enqueue(std::move(task));
        return result;
    }

    // В worker-е ожидание помогает пулу; get() вызывается отдельно для результата.
    template <class T>
    void wait(std::future<T>& future) {
        if (current_pool_ != this) {
            future.wait();
            return;
        }
        for (;;) {
            const auto status = future.wait_for(std::chrono::seconds(0));
            if (status == std::future_status::ready) return;
            if (status == std::future_status::deferred) { future.wait(); return; }
            if (!help_one()) {
                // Ожидаемый результат может быть опубликован без новой задачи.
                future.wait_for(std::chrono::milliseconds(1));
            }
        }
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
        std::size_t victim_start = 0;
        unsigned ticks = 0;
    };

    void enqueue(std::unique_ptr<detail::Task> task) noexcept;
    detail::Task* take_inbox(Worker& worker, detail::Inbox& inbox) noexcept;
    detail::Task* find_work(std::size_t index) noexcept;
    bool help_one() noexcept;
    void signal_work(bool all = false) noexcept;
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
    mutable std::mutex park_mutex_;
    mutable std::condition_variable park_cv_;
    std::atomic<std::size_t> wake_epoch_{0};
    std::size_t sleepers_ = 0; // Только под park_mutex_.

    static thread_local ThreadPool* current_pool_;
    static thread_local std::size_t current_worker_;
};

// Один управляющий поток на группу; пул должен пережить группу.
class TaskGroup {
public:
    explicit TaskGroup(ThreadPool& pool) : pool_(pool) {}
    TaskGroup(const TaskGroup&) = delete;
    TaskGroup& operator=(const TaskGroup&) = delete;
    ~TaskGroup() { try { wait(); } catch (...) {} }

    template <class F>
    void submit(F&& function) {
        futures_.emplace_back(); // Выделяем память до принятия задачи пулом.
        try {
            futures_.back() = pool_.submit(
                [fn = std::decay_t<F>(std::forward<F>(function))]() mutable {
                    std::invoke(fn);
                });
        } catch (...) {
            futures_.pop_back();
            throw;
        }
    }

    void wait() {
        std::exception_ptr failure;
        for (auto& future : futures_) {
            try {
                pool_.wait(future);
                future.get();
            } catch (...) {
                if (!failure) failure = std::current_exception();
            }
        }
        futures_.clear();
        if (failure) std::rethrow_exception(failure);
    }

private:
    ThreadPool& pool_;
    std::vector<std::future<void>> futures_;
};

} // namespace task_scheduler
