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
        workers_.back()->victim_start = i;
    }

    try {
        for (std::size_t i = 0; i < thread_count; ++i) {
            threads_.emplace_back([this, i] { run_worker(i); });
        }
    } catch (...) {
        stopping_.store(true, std::memory_order_release);
        signal_work(true);
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
    signal_work(true);
    for (auto& thread : threads_) {
        thread.join();
    }
}

void ThreadPool::signal_work(bool all) noexcept {
    // Мьютекс закрывает окно между проверкой предиката и фактическим wait.
    std::lock_guard<std::mutex> lock(park_mutex_);
    wake_epoch_.fetch_add(1, std::memory_order_release);
    if (all) park_cv_.notify_all();
    else if (sleepers_ != 0) park_cv_.notify_one();
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
        // Только распределение нагрузки, не публикация данных.
        const auto index = next_worker_.fetch_add(1, std::memory_order_relaxed)
                           % workers_.size();
        workers_[index]->inbox.push(raw_task);
    }
    // В том числе локальный push: дети нужны припаркованным ворам.
    signal_work();
}

detail::Task* ThreadPool::take_inbox(Worker& worker, detail::Inbox& inbox) noexcept {
    detail::Task* last = nullptr;
    auto* list = inbox.take_all(&last);
    if (list == nullptr) return nullptr;
    auto* result = list;
    list = list->next;
    for (unsigned n = 0; n < 32 && list != nullptr; ++n) {
        auto* next = list->next; // До публикации: после push узел может быть удалён.
        if (!worker.local.push(list)) break;
        list = next;
    }
    if (list != nullptr) {
        inbox.push_list(list, last);
        signal_work();
    }
    return result;
}

detail::Task* ThreadPool::find_work(std::size_t index) noexcept {
    auto& worker = *workers_[index];
    // Периодическая проверка inbox исключает голодание внешних задач.
    if (++worker.ticks == 32) {
        worker.ticks = 0;
        if (auto* task = take_inbox(worker, worker.inbox)) return task;
    }
    if (auto* task = worker.local.pop()) return task;
    if (auto* task = take_inbox(worker, worker.inbox)) return task;
    worker.victim_start = (worker.victim_start + 1) % workers_.size();
    for (std::size_t n = 0; n < workers_.size(); ++n) {
        const auto victim = (worker.victim_start + n) % workers_.size();
        if (victim == index) continue;
        if (auto* task = workers_[victim]->local.steal()) return task;
        if (auto* task = take_inbox(worker, workers_[victim]->inbox)) return task;
    }
    return nullptr;
}

void ThreadPool::execute(detail::Task* task) noexcept {
    {
        std::unique_ptr<detail::Task> owned_task(task);
        owned_task->run();
    }
    // acq_rel связывает результаты ВСЕХ завершённых задач с wait_idle().
    // Деструктор callable тоже должен закончиться до уменьшения счётчика.
    if (outstanding_.fetch_sub(1, std::memory_order_acq_rel) == 1) signal_work(true);
}

bool ThreadPool::help_one() noexcept {
    if (auto* task = find_work(current_worker_)) {
        execute(task);
        return true;
    }
    return false;
}

void ThreadPool::run_worker(std::size_t index) noexcept {
    current_pool_ = this;
    current_worker_ = index;
    detail::SpinWait idle;
    unsigned misses = 0;

    for (;;) {
        // Обязательно ДО поиска: иначе submit между поиском и снимком будет потерян.
        const auto epoch = wake_epoch_.load(std::memory_order_acquire);
        if (help_one()) {
            misses = 0;
            idle.reset();
            continue;
        }
        if (stopping_.load(std::memory_order_acquire) &&
            outstanding_.load(std::memory_order_acquire) == 0) {
            break;
        }
        if (++misses < 40) { idle.pause(); continue; }
        std::unique_lock<std::mutex> lock(park_mutex_);
        ++sleepers_;
        park_cv_.wait(lock, [&] {
            return wake_epoch_.load(std::memory_order_acquire) != epoch ||
                (stopping_.load(std::memory_order_acquire) &&
                 outstanding_.load(std::memory_order_acquire) == 0);
        });
        --sleepers_;
        misses = 0;
        idle.reset();
    }

    current_pool_ = nullptr;
}

void ThreadPool::wait_idle() const {
    if (current_pool_ == this) {
        throw std::logic_error("wait_idle cannot be called from this pool's task");
    }
    std::unique_lock<std::mutex> lock(park_mutex_);
    park_cv_.wait(lock, [&] { return outstanding_.load(std::memory_order_acquire) == 0; });
}

} // namespace task_scheduler
