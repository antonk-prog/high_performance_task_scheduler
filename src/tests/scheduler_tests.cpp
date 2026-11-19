#include "task_scheduler/thread_pool.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <class Exception, class F>
void require_throws(F&& action) {
    try {
        action();
    } catch (const Exception&) {
        return;
    }
    throw std::runtime_error("expected exception was not thrown");
}

void deque_boundaries() {
    require_throws<std::invalid_argument>([] {
        task_scheduler::detail::ChaseLevDeque<int> invalid(3);
    });
    task_scheduler::detail::ChaseLevDeque<int> queue(2);
    int first = 1;
    int second = 2;
    int third = 3;
    for (int round = 0; round < 10'000; ++round) {
        require(queue.pop() == nullptr, "empty pop");
        require(queue.steal() == nullptr, "empty steal");
        require(queue.push(&first), "first push");
        require(queue.push(&second), "second push");
        require(!queue.push(&third), "full queue must reject push");
        require(queue.pop() == &second, "owner must pop newest item");
        require(queue.steal() == &first, "thief must steal oldest item");
        require(queue.push(&third), "queue must be reusable");
        require(queue.pop() == &third, "owner must pop last item");
    }
}

void deque_owner_thieves_race() {
    constexpr std::size_t count = 200'000;
    struct Item { std::atomic<unsigned> seen{0}; };
    auto items = std::make_unique<Item[]>(count);
    task_scheduler::detail::ChaseLevDeque<Item> queue(8);
    std::atomic<bool> done{false};
    std::vector<std::thread> thieves;

    for (int t = 0; t < 4; ++t) {
        thieves.emplace_back([&] {
            for (;;) {
                if (auto* item = queue.steal()) {
                    item->seen.fetch_add(1, std::memory_order_relaxed);
                } else if (done.load(std::memory_order_acquire)) {
                    // После done новых push нет. Остаток заберут мы или другие воры.
                    while (auto* remaining = queue.steal()) {
                        remaining->seen.fetch_add(1, std::memory_order_relaxed);
                    }
                    break;
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
    for (std::size_t i = 0; i < count; ++i) {
        while (!queue.push(&items[i])) {
            if (auto* item = queue.pop()) {
                item->seen.fetch_add(1, std::memory_order_relaxed);
            }
        }
        if ((i & 1) == 0) {
            if (auto* item = queue.pop()) {
                item->seen.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }
    done.store(true, std::memory_order_release);
    for (auto& thief : thieves) {
        thief.join();
    }
    for (std::size_t i = 0; i < count; ++i) {
        require(items[i].seen.load() == 1, "deque lost or duplicated an item");
    }
}

void external_producers_exactly_once() {
    constexpr std::size_t producers = 6;
    constexpr std::size_t per_producer = 10'000;
    constexpr auto count = producers * per_producer;
    auto seen = std::make_unique<std::atomic<unsigned>[]>(count);
    std::vector<std::size_t> results(count, 0);
    for (std::size_t i = 0; i < count; ++i) {
        seen[i].store(0, std::memory_order_relaxed);
    }
    task_scheduler::ThreadPool pool(4, 8);
    std::vector<std::thread> threads;
    for (std::size_t p = 0; p < producers; ++p) {
        threads.emplace_back([&, p] {
            for (std::size_t n = 0; n < per_producer; ++n) {
                const auto i = p * per_producer + n;
                pool.submit([&, i] {
                    if (seen[i].fetch_add(1, std::memory_order_relaxed) == 0) {
                        results[i] = i + 1;
                    }
                });
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    pool.wait_idle();
    for (std::size_t i = 0; i < count; ++i) {
        require(seen[i].load() == 1, "pool lost or duplicated a task");
        require(results[i] == i + 1, "wait_idle did not publish task results");
    }
    require(pool.failed_tasks() == 0, "unexpected task failure");
}

void nested_overflow_and_move_only() {
    task_scheduler::ThreadPool pool(1, 2);
    std::size_t completed = 0; // Единственный worker, чтение после wait_idle.
    int result = 0;
    pool.submit([&] {
        for (int i = 0; i < 20'000; ++i) {
            pool.submit([&] { ++completed; });
        }
        pool.submit([value = std::make_unique<int>(42), &result] {
            result = *value;
        });
    });
    pool.wait_idle();
    require(completed == 20'000, "overflow tasks were lost");
    require(result == 42, "move-only task failed");
}

void other_workers_really_steal() {
    task_scheduler::ThreadPool pool(4, 1024);
    std::atomic<unsigned> stolen{0};
    std::atomic<bool> progressed{false};
    pool.submit([&] {
        const auto owner = std::this_thread::get_id();
        for (unsigned i = 0; i < 512; ++i) {
            pool.submit([&, owner] {
                if (std::this_thread::get_id() != owner) {
                    stolen.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }
        // Родитель удерживает владельца: дочерние задачи могут выполнить только воры.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (stolen.load(std::memory_order_relaxed) != 512 &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        progressed.store(stolen.load() == 512, std::memory_order_relaxed);
    });
    pool.wait_idle();
    require(progressed.load(), "other workers failed to steal from a busy owner");
}

void failures_and_worker_wait() {
    task_scheduler::ThreadPool pool(2, 4);
    std::atomic<bool> rejected{false};
    pool.submit([] { throw std::runtime_error("task failed"); });
    pool.submit([&] {
        try {
            pool.wait_idle();
        } catch (const std::logic_error&) {
            rejected.store(true, std::memory_order_relaxed);
        }
    });
    pool.wait_idle();
    require(pool.failed_tasks() == 1, "throwing task must be counted");
    require(rejected.load(), "worker wait_idle must be rejected");
    int result = 0;
    pool.submit([&] { result = 7; });
    pool.wait_idle();
    require(result == 7, "worker must survive a throwing task");
}

void destructor_drains_children() {
    std::atomic<unsigned> completed{0};
    {
        task_scheduler::ThreadPool pool(3, 4);
        for (int i = 0; i < 100; ++i) {
            pool.submit([&] {
                for (int n = 0; n < 100; ++n) {
                    pool.submit([&] { completed.fetch_add(1, std::memory_order_relaxed); });
                }
            });
        }
    }
    require(completed.load() == 10'000, "destructor failed to drain children");
}

void cross_pool_submission() {
    task_scheduler::ThreadPool first(2, 4);
    task_scheduler::ThreadPool second(2, 4);
    std::atomic<unsigned> completed{0};
    for (int i = 0; i < 2000; ++i) {
        first.submit([&] {
            second.submit([&] { completed.fetch_add(1, std::memory_order_relaxed); });
        });
    }
    first.wait_idle();
    second.wait_idle();
    require(completed.load() == 2000, "cross-pool submission failed");
}

void construction_and_idle() {
    require(task_scheduler::ThreadPool::default_thread_count() > 0, "default worker count");
    require_throws<std::invalid_argument>([] { task_scheduler::ThreadPool invalid(0); });
    require_throws<std::invalid_argument>([] { task_scheduler::ThreadPool invalid(1, 0); });
    require_throws<std::invalid_argument>([] { task_scheduler::ThreadPool invalid(1, 3); });
    for (int i = 0; i < 10; ++i) {
        task_scheduler::ThreadPool pool(2, 2);
        require(pool.thread_count() == 2, "incorrect worker count");
        pool.wait_idle();
        std::array<int, 64> results{};
        for (std::size_t n = 0; n < results.size(); ++n) {
            pool.submit([&, n] { results[n] = 1; });
        }
        pool.wait_idle();
        for (auto value : results) {
            require(value == 1, "repeated pool use lost work");
        }
    }
}

void inbox_multiple_consumers() {
    task_scheduler::detail::Inbox inbox;
    constexpr unsigned count = 40'000;
    std::atomic<unsigned> completed{0};
    std::atomic<unsigned> producers_left{4};
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < 4; ++i) {
        threads.emplace_back([&] {
            for (unsigned n = 0; n < count / 4; ++n) {
                auto fn = [&] { completed.fetch_add(1, std::memory_order_relaxed); };
                auto node = std::make_unique<task_scheduler::detail::CallableTask<decltype(fn)>>(fn);
                inbox.push(node.release());
            }
            producers_left.fetch_sub(1, std::memory_order_release);
        });
    }
    for (unsigned i = 0; i < 4; ++i) {
        threads.emplace_back([&] {
            for (;;) {
                const auto done = producers_left.load(std::memory_order_acquire) == 0;
                auto* list = inbox.take_all();
                if (!list && done) break;
                while (list) {
                    std::unique_ptr<task_scheduler::detail::Task> node(list);
                    list = node->next;
                    node->run();
                }
                std::this_thread::yield();
            }
        });
    }
    for (auto& thread : threads) thread.join();
    require(completed.load() == count, "MPMC inbox lost tasks");
    require(inbox.take_all() == nullptr, "inbox did not drain");
}

void inbox_segment_tail_and_order() {
    auto fn = [] {};
    task_scheduler::detail::CallableTask<decltype(fn)> a(fn), b(fn), c(fn), d(fn);
    task_scheduler::detail::Inbox inbox;
    inbox.push(&a); inbox.push(&b); inbox.push(&c);
    task_scheduler::detail::Task* tail = nullptr;
    auto* list = inbox.take_all(&tail);
    require(list == &c && list->next == &b && b.next == &a && tail == &a,
            "inbox LIFO and segment tail");
    inbox.push_list(&b, &a);
    inbox.push(&d);
    list = inbox.take_all(&tail);
    require(list == &d && d.next == &b && b.next == &a && tail == &a,
            "returned segment tail was lost");
    require(inbox.take_all(&tail) == nullptr && tail == nullptr, "empty segment tail");
}

void busy_owner_inboxes_and_overflow() {
    for (const auto capacity : {2U, 16U, 1024U}) {
        task_scheduler::ThreadPool pool(4, capacity);
        // Даём свободным worker-ам припарковаться до локальных push.
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        std::promise<void> entered, release;
        std::atomic<unsigned> children{0};
        auto released = release.get_future().share();
        auto parent = pool.submit([&] {
            for (unsigned n = 0; n < 1000; ++n) {
                pool.submit([&] { children.fetch_add(1, std::memory_order_relaxed); });
            }
            entered.set_value();
            released.wait(); // Удерживает владельца deque и overflow inbox.
        });
        entered.get_future().wait();
        std::vector<std::future<int>> shorts;
        for (int n = 0; n < 100; ++n) shorts.push_back(pool.submit([n] { return n; }));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        bool ready = true;
        for (auto& future : shorts) {
            if (future.wait_until(deadline) != std::future_status::ready) ready = false;
        }
        while (children.load() != 1000 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        ready = ready && children.load() == 1000;
        release.set_value(); // Освобождаем даже при провале, чтобы тест не зависал в destructor.
        parent.get();
        pool.wait_idle();
        require(ready, "short tasks stranded in busy owner's inbox");
        for (int n = 0; n < 100; ++n) require(shorts[n].get() == n, "future result mismatch");
    }
}

int fork_join(task_scheduler::ThreadPool& pool, unsigned depth) {
    if (depth == 0) return 1;
    int left = 0, right = 0;
    task_scheduler::TaskGroup group(pool);
    group.submit([&] { left = fork_join(pool, depth - 1); });
    group.submit([&] { right = fork_join(pool, depth - 1); });
    group.wait();
    return left + right;
}

void cooperative_wait_and_failures() {
    for (const auto workers : {1U, 2U, 4U}) {
        task_scheduler::ThreadPool pool(workers, 2);
        auto root = pool.submit([&] { return fork_join(pool, 8); });
        pool.wait(root);
        require(root.get() == 256, "nested TaskGroup deadlocked or lost results");
        auto parent = pool.submit([&] {
            auto child = pool.submit([value = std::make_unique<int>(42)]() mutable { return std::move(value); });
            pool.wait(child);
            require(*child.get() == 42, "move-only future result");
            auto deferred = std::async(std::launch::deferred, [] { return 9; });
            pool.wait(deferred);
            require(deferred.get() == 9, "deferred future wait");
            int completed = 0;
            task_scheduler::TaskGroup group(pool);
            group.submit([] { throw std::runtime_error("group failure"); });
            group.submit([&] { completed = 1; });
            require_throws<std::runtime_error>([&] { group.wait(); });
            require(completed == 1, "group failed to join after exception");
            group.submit([&] { completed = 2; });
            group.wait();
            require(completed == 2, "group reuse failed");
            { task_scheduler::TaskGroup implicit(pool); implicit.submit([&] { completed = 3; }); }
            require(completed == 3, "group destructor did not join");
        });
        parent.get();
        auto failed = pool.submit([]() -> int { throw std::logic_error("future failure"); });
        require_throws<std::logic_error>([&] { failed.get(); });
        int object = 7;
        auto reference = pool.submit([&]() -> int& { return object; });
        require(&reference.get() == &object, "reference result lost");
        pool.wait_idle();
        require(pool.failed_tasks() == 2, "exceptions must be counted once");
        struct ThrowOnMove {
            ThrowOnMove() = default;
            ThrowOnMove(ThrowOnMove&&) { throw std::runtime_error("move failed"); }
            void operator()() const {}
        };
        task_scheduler::TaskGroup rollback(pool);
        require_throws<std::runtime_error>([&] { rollback.submit(ThrowOnMove{}); });
        rollback.submit([] {});
        rollback.wait();
    }
}

void parked_workers_wake_repeatedly() {
    task_scheduler::ThreadPool pool(4, 2);
    for (unsigned round = 0; round < 100; ++round) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto future = pool.submit([&] {
            task_scheduler::TaskGroup group(pool);
            for (unsigned n = 0; n < 16; ++n) group.submit([] {});
            group.wait();
            return 17;
        });
        require(future.wait_for(std::chrono::seconds(5)) == std::future_status::ready,
                "lost wakeup across park/submit");
        require(future.get() == 17, "wake result");
        pool.wait_idle();
    }
}

} // namespace

int main() {
    struct Test { const char* name; void (*run)(); };
    const Test tests[] = {
        {"deque boundaries and ring reuse", deque_boundaries},
        {"deque owner/thieves race", deque_owner_thieves_race},
        {"multiple producers, exactly-once and visibility", external_producers_exactly_once},
        {"nested tasks, overflow and move-only captures", nested_overflow_and_move_only},
        {"actual work stealing", other_workers_really_steal},
        {"exceptions and worker wait rejection", failures_and_worker_wait},
        {"destructor drains child tasks", destructor_drains_children},
        {"submission to a different pool", cross_pool_submission},
        {"construction, empty wait and repeated use", construction_and_idle},
        {"inbox concurrent producers/consumers and deletion", inbox_multiple_consumers},
        {"inbox segment tail and order", inbox_segment_tail_and_order},
        {"busy owner inbox and small-ring overflow", busy_owner_inboxes_and_overflow},
        {"cooperative fork-join, results and exceptions", cooperative_wait_and_failures},
        {"repeated park and local/external wakeup", parked_workers_wake_repeatedly},
    };
    for (const auto& test : tests) {
        try {
            test.run();
            std::cout << "[PASS] " << test.name << std::endl;
        } catch (const std::exception& error) {
            std::cerr << "[FAIL] " << test.name << ": " << error.what() << '\n';
            return 1;
        }
    }
}
