#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>

namespace task_scheduler {
namespace detail {

// push/pop: только владелец. steal: любое количество других потоков.
// Кольцо не растёт: это позволяет обойтись без reclamation старых буферов.
template <class T>
class ChaseLevDeque {
public:
    explicit ChaseLevDeque(std::size_t capacity)
        : capacity_(check_capacity(capacity)),
          mask_(capacity_ - 1),
          slots_(std::make_unique<std::atomic<T*>[]>(capacity_)) {
        for (std::size_t i = 0; i < capacity_; ++i) {
            slots_[i].store(nullptr, std::memory_order_relaxed);
        }
    }

    ChaseLevDeque(const ChaseLevDeque&) = delete;
    ChaseLevDeque& operator=(const ChaseLevDeque&) = delete;

    // false означает, что кольцо заполнено; указатель остаётся у вызывающего.
    bool push(T* item) noexcept {
        const auto bottom = bottom_.load(std::memory_order_relaxed);
        const auto top = top_.load(std::memory_order_acquire);
        if (bottom - top >= static_cast<std::int64_t>(capacity_)) {
            return false;
        }

        slot(bottom).store(item, std::memory_order_relaxed);
        // Публикуем и указатель, и содержимое задачи.
        bottom_.store(bottom + 1, std::memory_order_release);
        return true;
    }

    T* pop() noexcept {
        const auto bottom = bottom_.load(std::memory_order_relaxed) - 1;
        bottom_.store(bottom, std::memory_order_relaxed);
        // В паре с fence в steal запрещает обоим потокам забрать последний item.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        auto top = top_.load(std::memory_order_relaxed);

        if (top > bottom) {
            bottom_.store(bottom + 1, std::memory_order_relaxed);
            return nullptr;
        }

        auto* item = slot(bottom).load(std::memory_order_relaxed);
        if (top == bottom) {
            // За последний item владелец тоже борется через CAS.
            if (!top_.compare_exchange_strong(top, top + 1,
                                             std::memory_order_seq_cst,
                                             std::memory_order_relaxed)) {
                item = nullptr;
            }
            bottom_.store(bottom + 1, std::memory_order_relaxed);
        }
        return item;
    }

    T* steal() noexcept {
        auto top = top_.load(std::memory_order_acquire);
        std::atomic_thread_fence(std::memory_order_seq_cst);
        const auto bottom = bottom_.load(std::memory_order_acquire);
        if (top >= bottom) {
            return nullptr;
        }

        // Слот может быть переиспользован, пока вор остановлен перед CAS.
        // Поэтому сам слот атомарный, а item нельзя разыменовать до успеха CAS.
        auto* item = slot(top).load(std::memory_order_relaxed);
        if (!top_.compare_exchange_strong(top, top + 1,
                                         std::memory_order_seq_cst,
                                         std::memory_order_relaxed)) {
            return nullptr;
        }
        return item;
    }

private:
    static std::size_t check_capacity(std::size_t capacity) {
        if (capacity < 2 || (capacity & (capacity - 1)) != 0 ||
            capacity > static_cast<std::size_t>(
                std::numeric_limits<std::int64_t>::max() / 2)) {
            throw std::invalid_argument("queue capacity must be a power of two >= 2");
        }
        return capacity;
    }

    std::atomic<T*>& slot(std::int64_t index) noexcept {
        return slots_[static_cast<std::size_t>(index) & mask_];
    }

    static_assert(std::atomic<T*>::is_always_lock_free,
                  "lock-free pointer atomics are required");
    static_assert(std::atomic<std::int64_t>::is_always_lock_free,
                  "lock-free 64-bit atomics are required");

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<std::atomic<T*>[]> slots_;
    alignas(64) std::atomic<std::int64_t> top_{0};
    alignas(64) std::atomic<std::int64_t> bottom_{0};
};

} // namespace detail
} // namespace task_scheduler
