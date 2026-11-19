#include "task_scheduler/thread_pool.hpp"

#include <cstddef>
#include <iostream>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

int main() {
    task_scheduler::ThreadPool pool(4);
    std::vector<int> values(10'000);

    // Два внешних потока одновременно отправляют задачи в pool.
    auto producer = [&](std::size_t begin, std::size_t end) {
        for (auto i = begin; i < end; ++i) {
            pool.submit([&, i] { values[i] = static_cast<int>(i); });
        }
    };
    std::thread first(producer, 0, values.size() / 2);
    std::thread second(producer, values.size() / 2, values.size());
    first.join();
    second.join();
    pool.wait_idle();

    std::cout << "Sum: " << std::accumulate(values.begin(), values.end(), 0LL) << '\n';

    auto answer = pool.submit([&pool] {
        auto child = pool.submit([value = std::make_unique<int>(42)] { return *value; });
        pool.wait(child);
        return child.get();
    });
    std::cout << "Nested task: " << answer.get() << '\n';
}
