import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
                2, // corePoolSize
                4, // maxPoolSize
                5, // keepAliveTime
                TimeUnit.SECONDS, // timeUnit
                5, // queueSize
                1, // minSpareThreads
                null, // threadFactory
                new CustomThreadPool.CallerRunsPolicy() // rejectionPolicy
        );

        for (int i = 0; i < 20; i++) {
            final int taskId = i;
            try {
                pool.execute(() -> {
                    System.out.printf("[Task-%d] Started in %s%n",
                            taskId, Thread.currentThread().getName());

                    try {
                        Thread.sleep(1000 + (int)(Math.random() * 2000));
                    } catch (InterruptedException e) {
                        System.out.printf("[Task-%d] Interrupted%n", taskId);
                    }

                    System.out.printf("[Task-%d] Completed%n", taskId);
                });

                Thread.sleep(200);
            } catch (RejectedExecutionException e) {
                System.out.printf("[Main] Task-%d rejected%n", taskId);
            }
        }

        Thread.sleep(5000);
        pool.shutdown();

        try {
            pool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            pool.shutdownNow();
        }

        System.out.println("\n=== Pool Statistics ===");
        System.out.println("Active threads: " + pool.getActiveThreads());
        System.out.println("Current pool size: " + pool.getCurrentPoolSize());
        System.out.println("Completed tasks: " + pool.getCompletedTasks());
        System.out.println("Rejected tasks: " + pool.getRejectedTasks());
        System.out.println("Task types distribution: " + pool.getTaskStatistics());
    }
}
