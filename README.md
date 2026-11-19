# Task scheduler — C++17

Пул потоков с локальными **Chase–Lev deque**, кражей задач и атомарными inbox.
Свободный worker забирает работу как из чужой deque, так и из чужого inbox.
`submit` возвращает future; кооперативное ожидание позволяет выполнять fork-join
даже на одном worker-е. В простое потоки паркуются через mutex/condition_variable.
Сборка — CMake, библиотека использует только стандартную библиотеку.

## Сборка и запуск

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/scheduler_example
```

## Использование

```cpp
#include "task_scheduler/thread_pool.hpp"
#include <iostream>
#include <memory>

int main() {
    task_scheduler::ThreadPool pool(4, 1024); // workers, размер локального кольца
    auto result = pool.submit([value = std::make_unique<int>(42)] { return *value; });
    std::cout << result.get() << '\n'; // Главный поток может ждать обычным get().

    auto parent = pool.submit([&] {
        int left = 0, right = 0;
        task_scheduler::TaskGroup children(pool);
        children.submit([&] { left = 20; });
        children.submit([&] { right = 22; });
        children.wait(); // Worker исполняет другие задачи во время ожидания.
        return left + right;
    });
    std::cout << parent.get() << '\n';
    pool.wait_idle();
}
```

- `submit(F)` принимает callable без аргументов, в том числе move-only, и возвращает
  `std::future<T>`. Поддерживаются `void`, ссылки и move-only результаты. Исключения
  доставляются в `future.get()` и один раз учитываются в `failed_tasks()`.
- Внутри worker-а для отдельного future используйте `pool.wait(future)`, затем
  `future.get()`. Сам `std::future::get()` не кооперативен: блокировка всех worker-ов
  обычным get не позволяет пулу выполнять ожидающих детей.
- `TaskGroup::wait()` дожидается **всех** задач группы, затем перебрасывает первое
  исключение. Группа переиспользуема; её деструктор дожидается задач без выброса.
  У группы один управляющий поток; concurrent submit/wait и добавление задач
  в родительскую группу из её детей не поддерживаются. Для рекурсии создавайте
  вложенные группы. Пул должен пережить группы и все обращения к нему.
- `ThreadPool()` использует `hardware_concurrency()`, минимум один worker.
  `ThreadPool(workers, capacity)`: workers > 0, capacity — степень двойки >= 2.
- `wait_idle()` вызывается **извне** и включает завершение детей и уничтожение
  callable-объектов; результаты задач после него видны вызывающему потоку.
  Из своей задачи он бросает `logic_error`: задача сама входит в pending.
- Деструктор дренирует все принятые задачи, включая детей, и join-ит потоки.
  Уничтожение пула собственным worker-ом вызывает `std::terminate()`.
- Перед wait_idle завершите внешних producers, если требуется дождаться всего
  отправленного ими набора; перед разрушением завершите все внешние обращения.
  Уже работающим задачам разрешено создавать детей во время drain.
- Захваченные ссылки должны жить до завершения задач. Пользовательские общие
  изменяемые данные требуют собственной синхронизации. Порядок выполнения не задан.

Ошибка выделения памяти или конструирования callable выходит из submit до принятия
задачи; счётчик pending не изменяется. `thread_count()` и `failed_tasks()` доступны
для диагностики. Готовность future означает готовность результата; для ожидания
уничтожения всех callable используйте внешний wait_idle.

## Как устроено

1. **Bounded Chase–Lev deque.** Только владелец делает push/pop с bottom (LIFO).
   Воры крадут с top (FIFO); последний элемент разрешается через SC CAS.
2. **Intrusive MPMC inbox.** CAS публикует список, exchange отдаёт весь inbox
   одному потребителю. Producer никогда не разыменовывает старую голову.
   Потребитель исполняет один элемент, переносит до 32 в свою deque, возвращает
   остаток в общий inbox **до вызова пользовательского кода**. Приватного batch нет.
   Inbox — LIFO; хвосты опубликованных сегментов кэшируются, поэтому возврат
   большого overflow не вызывает повторного линейного обхода на каждой порции.
3. **Поиск:** своя deque → свой inbox → чужие deque/inbox со сдвигающимся началом
   обхода. Раз в 32 поиска свой inbox проверяется первым против голодания.
4. **Парковка:** ~32 spin, ~8 yield, затем condvar. Epoch снимается **до** поиска.
   Публикация задачи, включая локальный push, меняет epoch под mutex и будит одного.
   Начало drain и завершение последней задачи будят всех.

```text
external producers ── round-robin ──> worker inbox <── overflow
                                         │             ↑
                     любой worker забирает список      │
                                         ↓             │
                                   local deque <── local submit
                                         │
                                    pop / steal
```

Фиксированное кольцо не ограничивает общее количество задач: overflow уходит в
доступный ворам inbox. Буфер живёт до join; список передаётся целиком через exchange.
Поэтому hazard pointers и retire-списки **сознательно заменены** протоколом владения.
Динамический рост кольца и публичные shutdown/shutdown_now **отложены**.
Graceful drain реализован деструктором; отмена и broken_promise не заявляются.

### Модель памяти и стоимость

Перед публикацией bottom в push стоит **release fence**: гарантия сохраняется
в C++20 даже при последующей relaxed-записи bottom из pop. Слоты дополнительно
используют release/acquire для явной публикации объекта, распознаваемой TSan.
SC fences/CAS разрешают гонку за последний элемент. Обоснования всех категорий
memory_order находятся в `src/task_scheduler/detail/chase_lev_deque.hpp`; протокол
inbox и счётчиков также прокомментирован. Индексы int64 не должны переполняться.

Очереди используют lock-free атомики (проверено static_assert), но **весь submit
не lock-free**: аллокатор, future и уведомление под park_mutex имеют свою стоимость.
Общие pending/round-robin счётчики тоже могут стать узким местом. Кооперативный
wait при отсутствии доступной работы ждёт future до 1 ms перед новым поиском.
Рекурсивное helping использует стек потока: для реальных алгоритмов нужен порог
мелкости задач (в сортировке — 1000 элементов).

## Тесты, sanitizers и coverage

`scheduler_tests` содержит 14 сценариев: deque boundaries/reuse и owner/thieves
race; несколько producers, consumers и немедленное удаление inbox-узлов; ровно
одно выполнение, видимость результатов; занятый владелец и capacity 2/16/1024;
move-only/reference/void futures; исключения и вложенный fork-join на 1/2/4 worker-ах;
повторные park/submit; drain детей; cross-pool submit и конструкторы.

На Linux `scheduler_failure_tests` инъецирует ошибку первого и частичного запуска
через interposition pthread_create, а отдельный процесс проверяет self-destruction.
Проверки работают в Release независимо от NDEBUG; CTest ограничивает время.

```bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DTASK_SCHEDULER_ASAN_UBSAN=ON
cmake --build build-asan -j
ASAN_OPTIONS=detect_leaks=1 UBSAN_OPTIONS=halt_on_error=1 ctest --test-dir build-asan --output-on-failure
ASAN_OPTIONS=detect_leaks=1 UBSAN_OPTIONS=halt_on_error=1 ./build-asan/scheduler_benchmark 4 10000 4 0 1

cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DTASK_SCHEDULER_TSAN=ON
cmake --build build-tsan -j
TSAN_OPTIONS=halt_on_error=1 ctest --test-dir build-tsan --output-on-failure
TSAN_OPTIONS=halt_on_error=1 ./build-tsan/scheduler_benchmark 4 10000 4 0 1

cmake -S . -B build-cxx20 -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=20
cmake --build build-cxx20 -j
ctest --test-dir build-cxx20 --output-on-failure
```

ASan/UBSan и TSan — отдельные сборки. GCC предупреждает, что TSan не моделирует
atomic_thread_fence; suppression нет, публикация slot дополнительно имеет явную
release/acquire-пару. Чистый TSan не заменяет доказательство алгоритма и проверки
на weak-memory машинах; такие машины в текущем прогоне не использовались.

### Покрытие

GCC 14.2 + gcovr 8.8, Linux x86-64: **229/229 строк (100%)**, **139/177 ветвей
(78,5%)** библиотеки. Включены `src/thread_pool.cpp` и все `src/task_scheduler/*.hpp`,
исключены только тесты, benchmark и STL. `--merge-lines` объединяет попадания разных
шаблонных инстанцирований в одну исходную строку; это не 100% всех инстанцирований
или всех ветвей. Счётчики coverage атомарные, чтобы конкурирующие workers не теряли
попадания. Failure/death tests входят в измерение. Число покрытых конкурентных
ветвей может немного меняться от запуска к запуску.

```bash
# Используйте свежий каталог для каждого нового замера.
cmake -S . -B build-coverage -DCMAKE_BUILD_TYPE=Debug -DTASK_SCHEDULER_COVERAGE=ON
cmake --build build-coverage -j
ctest --test-dir build-coverage --output-on-failure
# gcovr >= 8.8 устанавливается отдельно и не нужен для сборки библиотеки.
gcovr build-coverage --root . --filter 'src/thread_pool.cpp|src/task_scheduler/.*' \
  --merge-lines --txt-metric line --txt-metric branch --fail-under-line 100 --txt -
```

## Benchmark

```bash
# workers tasks/elements producers work odd-repeats
./build/scheduler_benchmark 4 1000000 4 32 5
./build/scheduler_benchmark 8 100000 4 0 5 # почти пустая задача
```

Эталон — `src/benchmarks/naive_pool.hpp`: одна общая LIFO deque, mutex, condvar,
packaged_task/future и кооперативный wait. LIFO уменьшает глубину helping относительно
FIFO; оба пула исполняют идентичные workloads. Сценарии:

- один/несколько внешних producers (их создание входит во время);
- recursive split: независимые листья, N scheduler-задач, join дерева через внешний
  wait_idle; это нагрузка на локальный submit, без ожидания каждого мелкого ребёнка;
- настоящий fork-join mergesort: два child future, кооперативный join перед merge,
  std::sort ниже порога 1000, проверка точного совпадения с последовательным эталоном.

Для каждого пула/сценария: один прогрев, пять повторов, медиана steady_clock.
Создание пула и данных, эталонный расчёт и проверка находятся вне таймера. Все
результаты проверяются поэлементно. `work` — число шагов xorshift, не калиброванные
микросекунды. В сортировке M/s означает **элементы**, в остальных строках — задачи.
Выводятся ms, M/s обоих пулов и отношение mutex/ws; в конце — idle CPU процесса.

### Фактические результаты

Linux x86-64, GCC 14.2, Release, Intel i7-12700H (20 логических CPU), без pinning;
load average около 1–2. Это собственные замеры на этой машине, не числа из ревью
на 60-ядерной машине. Все значения ниже — медиана пяти повторов после прогрева.
N = 100 000, producers = 4, work = 32; число worker-ов 32 превышает число CPU.

| Сценарий | 1 worker, M/s | 8 workers, M/s | 32 workers, M/s | mutex M/s при 32 | Ускорение при 32 |
|---|---:|---:|---:|---:|---:|
| Один producer | 2,843 | 1,742 | 1,658 | 0,152 | 10,91× |
| Четыре producers | 2,731 | 2,434 | 1,969 | 0,701 | 2,81× |
| Recursive split | 4,651 | 4,498 | 3,874 | 1,293 | 3,00× |
| Fork-join sort, млн элементов/s | 18,957 | 60,283 | 65,598 | 61,715 | 1,06× |

Миллион задач/элементов, **4 worker-а**, остальные параметры те же:

| Сценарий | Work-stealing, ms | Mutex, ms | Ускорение |
|---|---:|---:|---:|
| Один producer | 421,094 | 1159,479 | 2,75× |
| Четыре producers | 329,644 | 636,209 | 1,93× |
| Recursive split | 185,159 | 661,369 | 3,57× |
| Fork-join sort | 21,440 | 39,721 | 1,85× |

После 50 ms стабилизации пустой пул наблюдался 300 ms: процессное CPU время
округлилось до 0,000 s при 1/4/8/32 worker-ах. Это предел точности вывода, а не
утверждение абсолютного нуля. В отличие от busy-spin, ядра в простое не заняты.
Масштабирование не монотонно: более быстрый baseline не означает ускорение от
добавления worker-ов. Например, sort на одном worker-е слегка уступил эталону.

## Подключение и установка

```cmake
add_subdirectory(path/to/high_performance_task_scheduler)
target_link_libraries(your_target PRIVATE task_scheduler)
```

При вложенной сборке собирается только библиотека. Тесты отдельной сборки
отключаются через `-DBUILD_TESTING=OFF`. Предупреждения, sanitizer и coverage флаги PRIVATE.
Если потребитель явно выбирает инструментированную статическую библиотеку, он
должен сам подключить соответствующий runtime при линковке своего приложения.

```bash
cmake --install build --prefix /path/to/prefix
```

Устанавливаются библиотека и заголовки в стандартные каталоги lib/include.

## Файлы и OpenSpec

- `src/thread_pool.cpp`, `src/task_scheduler/thread_pool.hpp` — пул, future и TaskGroup.
- `src/task_scheduler/detail/` — deque, inbox, task и backoff с обоснованием memory_order.
- `src/tests/` — регрессии и Linux failure/death tests.
- `src/benchmarks/` — эталон и сравнительные сценарии.
- `openspec/changes/work-stealing-scheduler/` — восстановленные документы из 807f88e,
  актуальные решения и отмеченные tasks. Исходный proposal сохранён с пояснением
  изменения scope; design/specs приведены к текущему контракту. Рост кольца и
  shutdown_now оставлены явно отложенными; change ещё не архивирован.
