#include "task_scheduler/thread_pool.hpp"

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <exception>
#include <dlfcn.h>
#include <iostream>
#include <pthread.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
std::atomic<int> launches_before_failure{-1};
}

// Перехватываем также вызовы из динамической libstdc++.
extern "C" int pthread_create(pthread_t* thread, const pthread_attr_t* attr,
                             void* (*entry)(void*), void* argument) noexcept {
    static const auto real_create = reinterpret_cast<decltype(&pthread_create)>(
        dlsym(RTLD_NEXT, "pthread_create"));
    if (!real_create) std::abort();
    const auto remaining = launches_before_failure.load();
    if (remaining >= 0 && launches_before_failure.fetch_sub(1) == 0) return EAGAIN;
    return real_create(thread, attr, entry, argument);
}

#if defined(TASK_SCHEDULER_GCOV)
extern "C" void __gcov_dump();
#endif

int main() {
    for (const int allowed : {0, 2}) {
        launches_before_failure.store(allowed);
        bool threw = false;
        try { task_scheduler::ThreadPool pool(4); }
        catch (const std::system_error&) { threw = true; }
        launches_before_failure.store(-1);
        if (!threw) { std::cerr << "thread launch failure was ignored\n"; return 1; }
    }
    // На момент fork в родителе не должно быть живых worker-ов.
    const auto child = fork();
    if (child < 0) return 1;
    if (child == 0) {
        std::set_terminate([] {
#if defined(TASK_SCHEDULER_GCOV)
            __gcov_dump();
#endif
            std::_Exit(86);
        });
        auto pool = std::make_unique<task_scheduler::ThreadPool>(1);
        auto* raw = pool.release();
        raw->submit([raw] { delete raw; });
        for (;;) pause();
    }
    int status = 0;
    if (waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 86) {
        std::cerr << "self destruction must terminate\n";
        return 1;
    }
    std::cout << "[PASS] failed thread launches clean up; self destruction terminates\n";
}
