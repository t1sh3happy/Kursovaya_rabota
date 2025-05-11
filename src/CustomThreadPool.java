import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

public class CustomThreadPool implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;

    private final AtomicInteger currentPoolSize = new AtomicInteger(0);
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicInteger rejectedTasks = new AtomicInteger(0);

    private final List<BlockingQueue<Runnable>> workerQueues;
    private final List<Worker> workers;
    private final ThreadFactory threadFactory;
    private final RejectedExecutionHandler rejectionHandler;

    private volatile boolean isShutdown = false;
    private volatile boolean isTerminated = false;

    private final Lock mainLock = new ReentrantLock();
    private final Condition termination = mainLock.newCondition();

    private final Map<String, AtomicInteger> taskStats = new ConcurrentHashMap<>();

    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime,
                            TimeUnit timeUnit, int queueSize, int minSpareThreads,
                            ThreadFactory threadFactory, RejectedExecutionHandler rejectionHandler) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = threadFactory != null ? threadFactory : new DefaultThreadFactory();
        this.rejectionHandler = rejectionHandler != null ? rejectionHandler : new DefaultRejectedExecutionHandler();

        this.workerQueues = new ArrayList<>(maxPoolSize);
        this.workers = new ArrayList<>(maxPoolSize);

        for (int i = 0; i < corePoolSize; i++) {
            createWorker(true);
        }

        System.out.printf("[Pool] Initialized with corePoolSize=%d, maxPoolSize=%d, queueSize=%d, minSpareThreads=%d%n",
                corePoolSize, maxPoolSize, queueSize, minSpareThreads);
    }

    private void createWorker(boolean core) {
        if (isShutdown) {
            return;
        }

        mainLock.lock();
        try {
            if (currentPoolSize.get() >= maxPoolSize) {
                return;
            }

            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
            workerQueues.add(queue);

            Worker worker = new Worker(queue);
            Thread thread = threadFactory.newThread(worker);
            worker.setThread(thread);

            workers.add(worker);
            currentPoolSize.incrementAndGet();

            thread.start();

            System.out.printf("[ThreadFactory] Creating new thread: %s (core=%b)%n", thread.getName(), core);
        } finally {
            mainLock.unlock();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown) {
            rejectionHandler.rejectedExecution(command, this);
            return;
        }

        int active = activeThreads.get();
        int spareThreads = currentPoolSize.get() - active;

        if (spareThreads < minSpareThreads && currentPoolSize.get() < maxPoolSize) {
            createWorker(false);
        }

        int selectedQueue = -1;
        int minQueueSize = Integer.MAX_VALUE;

        for (int i = 0; i < workerQueues.size(); i++) {
            int size = workerQueues.get(i).size();
            if (size < minQueueSize && size < queueSize) {
                minQueueSize = size;
                selectedQueue = i;
            }
        }

        if (selectedQueue == -1 && currentPoolSize.get() < maxPoolSize) {
            createWorker(false);
            selectedQueue = workerQueues.size() - 1;
        }

        if (selectedQueue != -1) {
            BlockingQueue<Runnable> queue = workerQueues.get(selectedQueue);
            if (queue.offer(command)) {
                System.out.printf("[Pool] Task accepted into queue #%d: %s%n",
                        selectedQueue, command.toString());

                String taskType = command.getClass().getSimpleName();
                taskStats.computeIfAbsent(taskType, k -> new AtomicInteger(0)).incrementAndGet();

                return;
            }
        }

        rejectionHandler.rejectedExecution(command, this);
        rejectedTasks.incrementAndGet();
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> future = new FutureTask<>(callable);
        execute(future);
        return future;
    }

    @Override
    public void shutdown() {
        mainLock.lock();
        try {
            isShutdown = true;
            System.out.println("[Pool] Shutdown initiated");

            for (Worker worker : workers) {
                Thread t = worker.thread;
                if (!worker.isWorking && !t.isInterrupted()) {
                    t.interrupt();
                }
            }
        } finally {
            mainLock.unlock();
        }
    }

    @Override
    public void shutdownNow() {
        mainLock.lock();
        try {
            isShutdown = true;
            System.out.println("[Pool] ShutdownNow initiated");

            for (Worker worker : workers) {
                Thread t = worker.thread;
                if (!t.isInterrupted()) {
                    t.interrupt();
                }
            }

            for (BlockingQueue<Runnable> queue : workerQueues) {
                queue.clear();
            }
        } finally {
            mainLock.unlock();
        }
    }

    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        mainLock.lock();
        try {
            while (!isTerminated) {
                if (nanos <= 0) {
                    return;
                }
                nanos = termination.awaitNanos(nanos);
            }
        } finally {
            mainLock.unlock();
        }
    }

    public int getActiveThreads() {
        return activeThreads.get();
    }

    public int getCurrentPoolSize() {
        return currentPoolSize.get();
    }

    public int getCompletedTasks() {
        return completedTasks.get();
    }

    public int getRejectedTasks() {
        return rejectedTasks.get();
    }

    public Map<String, Integer> getTaskStatistics() {
        Map<String, Integer> result = new HashMap<>();
        for (Map.Entry<String, AtomicInteger> entry : taskStats.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }

    @Override
    public boolean isShutdown() {
        return isShutdown;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> queue;
        private Thread thread;
        private volatile boolean isWorking = false;

        public Worker(BlockingQueue<Runnable> queue) {
            this.queue = queue;
        }

        public void setThread(Thread thread) {
            this.thread = thread;
        }

        @Override
        public void run() {
            try {
                while (!isShutdown || !queue.isEmpty()) {
                    try {
                        Runnable task = queue.poll(keepAliveTime, timeUnit);

                        if (task != null) {
                            isWorking = true;
                            activeThreads.incrementAndGet();

                            System.out.printf("[Worker] %s executes %s%n",
                                    Thread.currentThread().getName(), task.toString());

                            try {
                                task.run();
                                completedTasks.incrementAndGet();
                            } catch (Exception e) {
                                System.out.printf("[Worker] %s task %s failed: %s%n",
                                        Thread.currentThread().getName(), task.toString(), e.getMessage());
                            } finally {
                                activeThreads.decrementAndGet();
                                isWorking = false;
                            }
                        } else if (currentPoolSize.get() > corePoolSize) {
                            System.out.printf("[Worker] %s idle timeout, stopping.%n",
                                    Thread.currentThread().getName());
                            break;
                        }
                    } catch (InterruptedException e) {
                        if (isShutdown && queue.isEmpty()) {
                            break;
                        }
                    }
                }
            } finally {
                mainLock.lock();
                try {
                    workers.remove(this);
                    workerQueues.remove(queue);
                    currentPoolSize.decrementAndGet();

                    System.out.printf("[Worker] %s terminated.%n",
                            Thread.currentThread().getName());

                    if (currentPoolSize.get() == 0) {
                        isTerminated = true;
                        termination.signalAll();
                    }
                } finally {
                    mainLock.unlock();
                }
            }
        }
    }

    private static class DefaultThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix = "CustomPool-worker-";

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            t.setDaemon(false);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    public interface RejectedExecutionHandler {
        void rejectedExecution(Runnable r, CustomExecutor executor);
    }

    private static class DefaultRejectedExecutionHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, CustomExecutor executor) {
            System.out.printf("[Rejected] Task %s was rejected due to overload!%n", r.toString());
            throw new RejectedExecutionException("Task " + r.toString() + " rejected from " + executor);
        }
    }

    public static class CallerRunsPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, CustomExecutor executor) {
            if (!executor.isShutdown()) {
                System.out.printf("[Rejected] Task %s will be executed in caller thread%n", r.toString());
                r.run();
            }
        }
    }

    public static class DiscardPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, CustomExecutor executor) {
            System.out.printf("[Rejected] Task %s was discarded%n", r.toString());
        }
    }
}

interface CustomExecutor extends Executor {
    void execute(Runnable command);
    <T> Future<T> submit(Callable<T> callable);
    void shutdown();
    void shutdownNow();
    boolean isShutdown();
}
