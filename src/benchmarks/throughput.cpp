#include "task_scheduler/thread_pool.hpp"
#include "naive_pool.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;

std::size_t number(const char* argument, bool zero = false) {
    const std::string_view text(argument);
    std::size_t value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || (!zero && value == 0)) {
        throw std::invalid_argument("arguments must be positive integers (work may be zero)");
    }
    return value;
}

std::uint64_t payload(std::size_t i, std::size_t work) {
    auto value = static_cast<std::uint64_t>(i) + 1;
    for (std::size_t n = 0; n < work; ++n) {
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
    }
    return value;
}

template <class Pool>
void split(Pool& pool, std::vector<std::uint64_t>& values,
           std::size_t begin, std::size_t end, std::size_t work) {
    if (end - begin == 1) { values[begin] = payload(begin, work); return; }
    const auto middle = begin + (end - begin) / 2;
    // Независимые листья; join дерева выполняет внешний wait_idle.
    pool.submit([&pool, &values, begin, middle, work] { split(pool, values, begin, middle, work); });
    split(pool, values, middle, end, work);
}

template <class Pool>
void sort(Pool& pool, std::vector<int>& values, std::size_t begin, std::size_t end) {
    auto first = values.begin() + static_cast<std::ptrdiff_t>(begin);
    auto last = values.begin() + static_cast<std::ptrdiff_t>(end);
    if (end - begin <= 1000) { std::sort(first, last); return; }
    const auto middle = begin + (end - begin) / 2;
    auto left = pool.submit([&, begin, middle] { sort(pool, values, begin, middle); });
    auto right = pool.submit([&, middle, end] { sort(pool, values, middle, end); });
    pool.wait(left);
    pool.wait(right);
    left.get(); right.get();
    std::inplace_merge(first, values.begin() + static_cast<std::ptrdiff_t>(middle), last);
}

template <class Pool>
double measure(std::size_t workers, std::size_t count, std::size_t producers,
               std::size_t work, unsigned scenario, std::size_t repeats) {
    Pool pool(workers);
    std::vector<std::uint64_t> expected(count);
    for (std::size_t i = 0; i < count; ++i) expected[i] = payload(i, work);
    std::vector<int> input(count);
    std::mt19937 random(42);
    for (auto& value : input) value = static_cast<int>(random() % 1'000'000);
    auto sorted = input;
    std::sort(sorted.begin(), sorted.end());
    std::vector<double> times;
    for (std::size_t repeat = 0; repeat <= repeats; ++repeat) {
        std::vector<std::uint64_t> values(count, 0);
        auto ints = input;
        const auto start = Clock::now();
        auto submit_range = [&](std::size_t begin, std::size_t end) {
            for (auto i = begin; i < end; ++i) pool.submit([&, i] { values[i] = payload(i, work); });
        };
        if (scenario < 2) {
            const auto senders = scenario == 0 ? 1 : producers;
            std::vector<std::thread> threads;
            for (std::size_t p = 0; p < senders; ++p) {
                const auto chunk = count / senders;
                const auto begin = chunk * p + std::min(p, count % senders);
                const auto end = begin + chunk + (p < count % senders ? 1 : 0);
                threads.emplace_back(submit_range, begin, end);
            }
            for (auto& thread : threads) thread.join();
        } else {
            auto root = pool.submit([&] {
                if (scenario == 2) split(pool, values, 0, count, work);
                else sort(pool, ints, 0, count);
            });
            root.get();
        }
        pool.wait_idle();
        const auto seconds = std::chrono::duration<double>(Clock::now() - start).count();
        if ((scenario == 3 && ints != sorted) || (scenario != 3 && values != expected)) {
            throw std::runtime_error("benchmark correctness check failed");
        }
        if (repeat != 0) times.push_back(seconds);
    }
    std::sort(times.begin(), times.end());
    return times[times.size() / 2];
}
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc > 6) throw std::invalid_argument(
            "usage: scheduler_benchmark [workers] [tasks/elements] [producers] [work] [odd repeats]");
        const auto workers = argc > 1 ? number(argv[1]) : 4;
        const auto tasks = argc > 2 ? number(argv[2]) : 1'000'000;
        const auto producers = argc > 3 ? number(argv[3]) : 4;
        const auto work = argc > 4 ? number(argv[4], true) : 32;
        const auto repeats = argc > 5 ? number(argv[5]) : 5;
        if (repeats % 2 == 0) throw std::invalid_argument("repeats must be odd");
        std::cout << "workers=" << workers << " tasks/elements=" << tasks << " producers="
                  << producers << " work=" << work << " warmup=1 repeats=" << repeats << '\n';
        std::cout << "scenario             ws_ms    mutex_ms    ws_M/s    mutex_M/s    speedup\n";
        const char* names[] = {"single producer", "multiple producers", "recursive split", "fork-join sort"};
        for (unsigned scenario = 0; scenario < 4; ++scenario) {
            const auto ws = measure<task_scheduler::ThreadPool>(workers, tasks, producers, work, scenario, repeats);
            const auto naive = measure<NaivePool>(workers, tasks, producers, work, scenario, repeats);
            std::cout << std::left << std::setw(21) << names[scenario] << std::right << std::fixed
                      << std::setprecision(3) << ws * 1000 << "    " << naive * 1000 << "    "
                      << static_cast<double>(tasks) / ws / 1e6 << "    "
                      << static_cast<double>(tasks) / naive / 1e6 << "    " << naive / ws << '\n';
        }
        // CPU процесса / wall-time: число занятых ядер, а не процент одного worker-а.
        task_scheduler::ThreadPool idle_pool(workers);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        const auto cpu = std::clock();
        const auto wall = Clock::now();
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        const auto cpu_seconds = static_cast<double>(std::clock() - cpu) / CLOCKS_PER_SEC;
        const auto wall_seconds = std::chrono::duration<double>(Clock::now() - wall).count();
        std::cout << "idle_cpu_seconds=" << cpu_seconds << " idle_wall_seconds=" << wall_seconds
                  << " occupied_cores=" << cpu_seconds / wall_seconds << '\n';
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
