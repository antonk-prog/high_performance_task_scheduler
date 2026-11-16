#include "task_scheduler/thread_pool.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

namespace {

std::size_t positive_number(const char* argument) {
    const std::string_view text(argument);
    std::size_t value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || value == 0) {
        throw std::invalid_argument("arguments must be positive integers");
    }
    return value;
}

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc > 5) {
            throw std::invalid_argument("usage: scheduler_benchmark [workers] [tasks] [producers] [work]");
        }
        const auto workers = argc > 1 ? positive_number(argv[1]) : 4;
        const auto tasks = argc > 2 ? positive_number(argv[2]) : 200'000;
        const auto producers = argc > 3 ? positive_number(argv[3]) : 4;
        const auto work = argc > 4 ? positive_number(argv[4]) : 32;
        task_scheduler::ThreadPool pool(workers);
        std::vector<std::uint64_t> results(tasks);
        pool.submit([] {});
        pool.wait_idle();

        std::cout << "workers=" << workers << " tasks=" << tasks
                  << " producers=" << producers << " work=" << work << '\n';

        auto submit_range = [&](std::size_t begin, std::size_t end) {
            for (auto i = begin; i < end; ++i) {
                pool.submit([&, i] {
                    auto value = static_cast<std::uint64_t>(i) + 1;
                    for (std::size_t n = 0; n < work; ++n) {
                        value ^= value >> 12;
                        value ^= value << 25;
                        value ^= value >> 27;
                    }
                    results[i] = value;
                });
            }
        };

        auto measure = [&](const char* name, auto&& submit) {
            std::fill(results.begin(), results.end(), 0);
            const auto begin = std::chrono::steady_clock::now();
            submit();
            pool.wait_idle();
            const auto seconds = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - begin).count();
            const auto checksum = std::accumulate(results.begin(), results.end(), std::uint64_t{0});
            std::cout << std::left << std::setw(20) << name << std::right
                      << std::fixed << std::setprecision(2)
                      << seconds * 1000.0 << " ms, "
                      << static_cast<double>(tasks) / seconds / 1'000'000.0
                      << " M tasks/s, checksum=" << checksum << '\n';
            return checksum;
        };

        const auto single = measure("single producer", [&] { submit_range(0, tasks); });
        const auto multi = measure("multiple producers", [&] {
            std::vector<std::thread> threads;
            threads.reserve(producers);
            for (std::size_t p = 0; p < producers; ++p) {
                // Разбиение без переполнения tasks * p.
                const auto chunk = tasks / producers;
                const auto remainder = tasks % producers;
                const auto begin = chunk * p + std::min(p, remainder);
                const auto end = begin + chunk + (p < remainder ? 1 : 0);
                threads.emplace_back(submit_range, begin, end);
            }
            for (auto& thread : threads) {
                thread.join();
            }
        });
        const auto nested = measure("nested / stealing", [&] {
            pool.submit([&] { submit_range(0, tasks); });
        });
        if (single != multi || single != nested || pool.failed_tasks() != 0) {
            throw std::runtime_error("benchmark results disagree");
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
