#pragma once

#include "task_scheduler/detail/task.hpp"

#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

// Эталон: одна общая очередь, mutex и condvar. LIFO, чтобы кооперативный
// рекурсивный join не разворачивал всё дерево в стек ожидающих родителей.
class NaivePool {
public:
    explicit NaivePool(std::size_t count) {
        try {
            for (std::size_t n = 0; n < count; ++n) threads_.emplace_back([this] { run(); });
        } catch (...) { stop(); throw; }
    }
    ~NaivePool() { stop(); }
    NaivePool(const NaivePool&) = delete;
    NaivePool& operator=(const NaivePool&) = delete;

    template <class F>
    auto submit(F&& fn) -> std::future<std::invoke_result_t<std::decay_t<F>&>> {
        std::packaged_task<std::invoke_result_t<std::decay_t<F>&>()> packaged(std::forward<F>(fn));
        auto future = packaged.get_future();
        auto task = std::make_unique<task_scheduler::detail::CallableTask<decltype(packaged)>>(std::move(packaged));
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks_.push_back(std::move(task));
            ++pending_;
        }
        cv_.notify_one();
        return future;
    }

    template <class T>
    void wait(std::future<T>& future) {
        if (current_ != this) { future.wait(); return; }
        while (future.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
            if (auto task = take()) execute(std::move(task));
            else future.wait_for(std::chrono::milliseconds(1));
        }
    }
    void wait_idle() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return pending_ == 0; });
    }

private:
    using Task = std::unique_ptr<task_scheduler::detail::Task>;
    Task take() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (tasks_.empty()) return {};
        auto task = std::move(tasks_.back());
        tasks_.pop_back();
        return task;
    }
    void execute(Task task) {
        task->run();
        task.reset();
        std::lock_guard<std::mutex> lock(mutex_);
        if (--pending_ == 0) cv_.notify_all();
    }
    void run() {
        current_ = this;
        for (;;) {
            Task task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [&] { return !tasks_.empty() || (stopping_ && pending_ == 0); });
                if (tasks_.empty()) break;
                task = std::move(tasks_.back());
                tasks_.pop_back();
            }
            execute(std::move(task));
        }
        current_ = nullptr;
    }
    void stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        cv_.notify_all();
        for (auto& thread : threads_) thread.join();
    }
    inline static thread_local NaivePool* current_ = nullptr;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<Task> tasks_;
    std::size_t pending_ = 0;
    bool stopping_ = false;
    std::vector<std::thread> threads_;
};
