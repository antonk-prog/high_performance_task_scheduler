#pragma once

#include <thread>

#if defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace task_scheduler {
namespace detail {

class SpinWait {
public:
    void pause() noexcept {
        if (attempts_++ >= 32) {
            std::this_thread::yield();
            return;
        }
#if defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64)
        _mm_pause();
#endif
    }

    void reset() noexcept { attempts_ = 0; }

private:
    unsigned attempts_ = 0;
};

} // namespace detail
} // namespace task_scheduler
