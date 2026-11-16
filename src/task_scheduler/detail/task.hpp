#pragma once

#include <functional>
#include <utility>

namespace task_scheduler {
namespace detail {

struct Task {
    Task* next = nullptr; // Используется только входящей очередью.
    virtual ~Task() = default;
    virtual void run() = 0;
};

template <class F>
struct CallableTask final : Task {
    template <class U>
    explicit CallableTask(U&& function) : function_(std::forward<U>(function)) {}

    void run() override { std::invoke(function_); }

    F function_;
};

} // namespace detail
} // namespace task_scheduler
