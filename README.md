# Concurrency & Multithreading in Java

My notes and practice code for Java concurrency: first the fundamentals (threads, `ExecutorService`, building a thread pool), then **concurrency questions asked in Rubrik interviews**: thread-safe data structures, custom locks, thread pools, schedulers, rate limiters and more, each built by hand from locks and conditions.

## Running

Requires **Java 25** (set in `build.gradle`).

```bash
./gradlew compileJava                       # build everything
```

In IntelliJ, open a file that has a `main` method and click ▶ next to it. From the terminal:

```bash
./gradlew compileJava
java -cp build/classes/java/main com.example.concurrency.fundamentals.threadpool.ThreadPoolDemo
```

## Project layout

All code lives under `src/main/java/com/example/concurrency/`:

```
fundamentals/                  what I learned first
├── threads/                   3 ways to create a thread
├── executorservice/           the java.util.concurrent ExecutorService API
└── threadpool/                a thread pool written from scratch

interview/                     questions asked in Rubrik interviews
├── datastructures/            thread-safe data structures
│   ├── blockingqueue/
│   └── linkedlist/
├── locks/                     building locks from lower-level primitives
│   └── readwritelock/
├── executors/                 custom thread pools / executors
│   └── boundedthreadpool/
├── scheduling/                running work in order, on time, or with dependencies
│   ├── delayeddispatcher/
│   └── jobscheduler/
├── resourcesharing/           limiting who can use a shared resource, and when
│   ├── fairplayground/
│   └── ratelimiter/
└── filecopy/                  splitting I/O work across threads
    └── preadpwrite/
```

### Naming conventions

| Pattern | Meaning |
|---|---|
| `Foo` | Main version, using `ReentrantLock` + `Condition` |
| `FooSync` | Same problem, solved with `synchronized` + `wait()` / `notifyAll()` |
| `FooSemaphore` | Same problem, solved with `Semaphore` |
| `FooPractice` | My own practice attempt; may be incomplete |
| `FooDemo` | Has a `main` method that runs and checks `Foo` |
| `mockinterview/` | Code written under mock-interview conditions |

---

## Fundamentals

### `fundamentals/threads`: creating threads

| Class | What it shows |
|---|---|
| `ThreadUsingThreadClass` | Subclass `Thread` and override `run()` |
| `ThreadUsingRunnable` | Implement `Runnable` and pass it to `new Thread(...)` |
| `ThreadUsingLambda` | The same, with a lambda as the `Runnable` |

### `fundamentals/executorservice`: the ExecutorService API

| Class | What it shows |
|---|---|
| `ExecuteRunnable` | `execute(Runnable)`: fire and forget |
| `SubmitRunnable` | `submit(Runnable)`: returns a `Future` so you can wait for it |
| `SubmitCallable` | `submit(Callable)`: `Future.get()` returns the task's result |
| `InvokeAll` | Run a list of `Callable`s and get every result |
| `InvokeAny` | Get the result of whichever task finishes first |
| `CancelTask` | `Future.cancel()` on a task that hasn't started |
| `ShutdownExample` | `shutdown()` vs `shutdownNow()` vs `awaitTermination()` |

### `fundamentals/threadpool`: a thread pool from scratch

| Class | What it is |
|---|---|
| `ThreadPool` | Holds a `BlockingQueue` of tasks and starts N worker threads |
| `PoolThreadRunnable` | The worker: loops forever, `take()`s a task from the queue and runs it |
| `ThreadPoolDemo` | Submits 10 tasks to a pool of 3 threads |

---

## Rubrik interview questions

Every problem in `interview/` is a concurrency question asked in Rubrik interviews. Most are solved from scratch with low-level primitives (`ReentrantLock`, `Condition`, `synchronized`, `Semaphore`) rather than ready-made `java.util.concurrent` classes.

**Confidence** is my own rating of how well I know each solution: ✅ confident, 🔁 needs revisiting.

### Thread-safe data structures: `interview/datastructures`

| Class | Description | Built with | Confidence |
|---|---|---|---|
| `blockingqueue/BoundedBlockingQueue` | Fixed-size circular-buffer queue; `put` waits while full, `take` waits while empty | `ReentrantLock`, 2 `Condition`s | ✅ |
| `blockingqueue/BoundedBlockingQueueSync` | Same queue | `synchronized`, `wait`/`notifyAll` | ✅ |
| `blockingqueue/BoundedBlockingQueueSemaphore` | Same queue | 3 `Semaphore`s (free slots, filled slots, mutex) | ✅ |
| `blockingqueue/BlockingQueueWithTimeout` | Adds `poll(timeout)` and `drain()` | `ReentrantLock`, `awaitNanos` | ✅ |
| `ConcurrentStack` | Linked-list stack with non-blocking `pop()` and blocking `popBlocking()` | `ReentrantLock`, `Condition` | ✅ |
| `LRUCache` | Least-recently-used cache: `HashMap` plus a doubly linked list | `ReentrantLock` | ✅ |
| `linkedlist/ConcurrentLinkedList` | List where searches run in parallel, one insert can run alongside searches, and delete runs alone | custom lock below | ✅ |
| `linkedlist/SearchInsertDeleteLock` | The search/insert/delete lock rules | `ReentrantLock`, 3 `Condition`s | ✅ |
| `linkedlist/SearchInsertDeleteFairLock` | Same rules, but waiting deleters go first so they can't starve | `ReentrantLock`, 3 `Condition`s | ✅ |
| `SnapshotKeyValueStore` | Key-value store with `takeSnapshot()` and reads "as of" a snapshot | `ReentrantReadWriteLock`, `TreeMap` versions | 🔁 |
| `TwoQueuesSharedBuffer` | Two FIFO queues sharing one preallocated buffer of blocks | `ReentrantLock` | 🔁 |

### Locks: `interview/locks`

| Class | Description | Built with | Confidence |
|---|---|---|---|
| `readwritelock/ReadWriteLock` | Many readers **or** one writer | `ReentrantLock`, 2 `Condition`s | ✅ |
| `readwritelock/ReadWriteLockSync` | Same lock | `synchronized`, `wait`/`notifyAll` | ✅ |
| `readwritelock/SharedCounter` | Counter protected by the lock above | `ReadWriteLock` | ✅ |
| `readwritelock/mockinterview/*` | The same problem, written in a mock interview | `ReentrantLock` | ✅ |

### Executors: `interview/executors`

| Class | Description | Built with | Confidence |
|---|---|---|---|
| `boundedthreadpool/BoundedThreadPool` | Thread pool with a bounded queue, `shutdown()`, `shutdownNow()`, `awaitTermination()` | `ReentrantLock`, `Condition` | ✅ |
| `boundedthreadpool/BoundedThreadPoolSync` | Same pool | `synchronized`, `wait`/`notifyAll` | ✅ |
| `boundedthreadpool/BoundedThreadPoolPractice` | My practice attempt at the same pool | `ReentrantLock`, `Condition` | ✅ |
| `PriorityExecutor` | Runs higher-priority tasks first | `PriorityQueue`, `ReentrantLock` | — |
| `TenantTaskExecutor` | Shared worker pool, at most K running tasks per tenant | `ReentrantLock`, per-tenant queues | — |
| `WorkerPool` | `submitWork()` + `blockUntilAllDone()`, collects failures | `ReentrantLock`, pending counter | 🔁 |

### Scheduling: `interview/scheduling`

| Class | Description | Built with | Confidence |
|---|---|---|---|
| `delayeddispatcher/DelayedDispatcher` | Runs each task after a delay; one dispatcher thread hands due tasks to a pool | `PriorityQueue` by run time, `Condition.await(timeout)` | 🔁 |
| `delayeddispatcher/DelayedDispatcherSync` | Same dispatcher | `synchronized`, `wait(timeout)` | 🔁 |
| `jobscheduler/JobScheduler` | Runs tasks whose dependencies come from a `TaskSource`, with N workers | `ReentrantLock`, `Condition` | ✅ |
| `jobscheduler/JobSchedulerSync` | Same scheduler | `synchronized`, `wait`/`notifyAll` | ✅ |
| `WebCrawler` | Multithreaded crawl of one host; knows when it is done (queue empty and nobody fetching) | `ReentrantLock`, `Condition` | 🔁 |

### Resource sharing: `interview/resourcesharing`

| Class | Description | Built with | Confidence |
|---|---|---|---|
| `ConnectionPool` | Borrow and return a fixed set of connections, waiting with a timeout | `ReentrantLock`, `Condition` | — |
| `ParkingLot` | Vehicles need `size` free slots in a row; wait until they fit | `ReentrantLock`, `Condition` | ✅ |
| `fairplayground/FairPlayground` | Only one team on the playground at a time, max N players, teams take turns | `ReentrantLock`, `Condition` | ✅ |
| `fairplayground/FairPlaygroundSync` | Same playground | `synchronized`, `wait`/`notifyAll` | ✅ |
| `ratelimiter/TokenBucketRateLimiter` | Token bucket: steady refill rate, bursts up to capacity | `ReentrantLock`, `Condition` | 🔁 |
| `ratelimiter/SlidingWindowRateLimiter` | At most N requests in any window of W ms | `ReentrantLock`, queue of expiry times | 🔁 |
| `BookingService` | Seat booking with optimistic concurrency: read, slow work without the lock, then validate the version and write | `synchronized` per seat, version number | 🔁 |

### File copy: `interview/filecopy`

| Class | Description | Built with | Confidence |
|---|---|---|---|
| `ParallelBlockCopy` | N threads each copy the next block number until the end of the file | `ReentrantLock` | 🔁 |
| `FloppyToSsd` | Many readers feed one writer that writes in order, in 64 KB batches | `ArrayBlockingQueue`, `ReentrantLock` | 🔁 |
| `preadpwrite/ParallelCopy` | Chunked copy where `pread` may return fewer bytes than asked | `ReentrantLock` | 🔁 |
| `FakeFS`, `preadpwrite/FakeFile` | In-memory stand-ins for the file APIs the interview provides | `synchronized` | — |

---

## Patterns used throughout

- **Always wait in a `while` loop**, never an `if`: `while (!condition) cond.await();`. The thread re-checks after waking, because wake-ups can be spurious or another thread may have got there first.
- **Do slow work outside the lock.** Workers `take()` a task under the lock, release it, run the task, then call `finish()` under the lock again.
- **Worker loop shape**, shared by the thread pools, scheduler, crawler and file copy:
  ```java
  while (true) {
      Task t = take();        // waits; returns null when there is no more work
      if (t == null) return;
      try { t.run(); finish(null); } catch (Exception e) { finish(e); }
  }
  ```
- **Call `lock()` before `try`, and `unlock()` in `finally`.** If `lock()` throws, `unlock()` is never called on a lock you don't hold.
- **`signal()` vs `signalAll()`**: use `signal()` when exactly one waiter can make progress (one free slot); use `signalAll()` when the condition may now be true for several waiters, or when different kinds of waiters share one condition (`synchronized` always has a single wait set, so it uses `notifyAll()`).
- **Two-phase shutdown**: set a flag under the lock and wake everyone, then `join()` the workers **outside** the lock.

