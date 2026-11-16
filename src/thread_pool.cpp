#include "task_scheduler/thread_pool.hpp"
#include "task_scheduler/detail/spin_wait.hpp"

#include <exception>
#include <stdexcept>

namespace task_scheduler {

thread_local ThreadPool* ThreadPool::current_pool_ = nullptr;
thread_local std::size_t ThreadPool::current_worker_ = 0;

std::size_t ThreadPool::default_thread_count() noexcept {
    const auto count = std::thread::hardware_concurrency();
    return count == 0 ? 1 : count;
}

ThreadPool::ThreadPool(std::size_t thread_count, std::size_t queue_capacity) {
    if (thread_count == 0) {
        throw std::invalid_argument("thread count must be greater than zero");
    }

    workers_.reserve(thread_count);
    threads_.reserve(thread_count);
    // Все очереди должны существовать до запуска первого вора.
    for (std::size_t i = 0; i < thread_count; ++i) {
        workers_.push_back(std::make_unique<Worker>(queue_capacity));
    }

    try {
        for (std::size_t i = 0; i < thread_count; ++i) {
            threads_.emplace_back([this, i] { run_worker(i); });
        }
    } catch (...) {
        stopping_.store(true, std::memory_order_release);
        for (auto& thread : threads_) {
            thread.join();
        }
        throw;
    }
}

ThreadPool::~ThreadPool() {
    // Нельзя уничтожать пул из его собственной задачи (join самого себя).
    if (current_pool_ == this) {
        std::terminate();
    }
    // Внешние producers к этому моменту уже должны завершиться.
    // Работающие задачи всё ещё могут добавлять дочерние задачи.
    stopping_.store(true, std::memory_order_release);
    for (auto& thread : threads_) {
        thread.join();
    }
}

void ThreadPool::enqueue(std::unique_ptr<detail::Task> task) noexcept {
    // Считаем задачу ДО публикации: worker может выполнить её немедленно.
    outstanding_.fetch_add(1, std::memory_order_relaxed);
    auto* raw_task = task.release();

    if (current_pool_ == this) {
        auto& worker = *workers_[current_worker_];
        if (!worker.local.push(raw_task)) {
            worker.inbox.push(raw_task);
        }
    } else {
        const auto index = next_worker_.fetch_add(1, std::memory_order_relaxed)
                           % workers_.size();
        workers_[index]->inbox.push(raw_task);
    }
}

void ThreadPool::refill_local(Worker& worker) noexcept {
    if (worker.batch == nullptr) {
        worker.batch = worker.inbox.take_all();
    }

    // Ограничиваем batch, чтобы не задерживать выполнение и кражу задач.
    for (unsigned i = 0; i < 32 && worker.batch != nullptr; ++i) {
        auto* task = worker.batch;
        auto* next = task->next;
        if (!worker.local.push(task)) {
            break;
        }
        // После push задача уже может быть выполнена и удалена вором.
        worker.batch = next;
    }
}

void ThreadPool::execute(detail::Task* task) noexcept {
    {
        std::unique_ptr<detail::Task> owned_task(task);
        try {
            owned_task->run();
        } catch (...) {
            failed_tasks_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    // acq_rel связывает результаты ВСЕХ завершённых задач с wait_idle().
    // Деструктор callable тоже должен закончиться до уменьшения счётчика.
    outstanding_.fetch_sub(1, std::memory_order_acq_rel);
}

void ThreadPool::run_worker(std::size_t index) noexcept {
    current_pool_ = this;
    current_worker_ = index;
    auto& worker = *workers_[index];
    detail::SpinWait idle;
    unsigned ticks = 0;
    std::size_t victim_start = index;

    for (;;) {
        // Даже при непрерывной локальной работе проверяем внешние задачи.
        if (++ticks == 32) {
            refill_local(worker);
            ticks = 0;
        }

        auto* task = worker.local.pop();
        if (task == nullptr) {
            refill_local(worker);
            task = worker.local.pop();
        }
        if (task == nullptr) {
            // Сдвигаем начало обхода, чтобы все воры не били в одну очередь.
            victim_start = (victim_start + 1) % workers_.size();
            for (std::size_t n = 0; n < workers_.size(); ++n) {
                const auto victim = (victim_start + n) % workers_.size();
                if (victim != index) {
                    task = workers_[victim]->local.steal();
                    if (task != nullptr) {
                        break;
                    }
                }
            }
        }

        if (task != nullptr) {
            idle.reset();
            execute(task);
        } else {
            if (stopping_.load(std::memory_order_acquire) &&
                outstanding_.load(std::memory_order_acquire) == 0) {
                break;
            }
            idle.pause();
        }
    }

    current_pool_ = nullptr;
}

void ThreadPool::wait_idle() const {
    if (current_pool_ == this) {
        throw std::logic_error("wait_idle cannot be called from this pool's task");
    }
    detail::SpinWait idle;
    while (outstanding_.load(std::memory_order_acquire) != 0) {
        idle.pause();
    }
}

} // namespace task_scheduler
