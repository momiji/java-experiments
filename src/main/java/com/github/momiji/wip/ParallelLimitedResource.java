package com.github.momiji.wip;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ParallelLimitedResource<T, R> {
    private final Stream<T> stream;
    private final Semaphore semaphore;
    private final Semaphore results = new Semaphore(0);
    private final ExecutorService executor;
    private final boolean sort;
    private final Task<T, R> task;
    private final BlockingQueue<CompletableFuture<Result<R>>> futureQueue = new LinkedBlockingQueue<>();
    private final CompletableFuture<Result<R>> POISON_PILL = new CompletableFuture<>();
    private final AtomicReference<Exception> isAborted = new AtomicReference<>(null);

    public ParallelLimitedResource(Stream<T> stream, int nThreads, boolean sort, Task<T, R> task) {
        this.stream = stream;
        this.semaphore = new Semaphore(nThreads);
        this.executor = Executors.newFixedThreadPool(nThreads);
        this.sort = sort;
        this.task = task;
    }

    public Stream<Result<R>> process() {
        Spliterator<T> spliterator = stream.spliterator();
        Iterator<T> iterator = Spliterators.iterator(spliterator);

        CompletableFuture.runAsync(() -> {
            try {
                while (iterator.hasNext()) {
                    if (isAborted.get() != null) {
                        break;
                    }
                    T item = iterator.next();
                    CompletableFuture<Result<R>> future = CompletableFuture.supplyAsync(() -> {
                        try {
                            semaphore.acquire();
                            if (isAborted.get() != null) {
                                results.release();
                                return new Result<>(null, semaphore, isAborted.get());
                            }
                            R result = task.accept(item);
                            results.release();
                            return new Result<>(result, semaphore, null);
                        } catch (Exception e) {
                            results.release();
                            return new Result<>(null, null, abort(e));
                        }
                    }, executor);
                    futureQueue.add(future);
                }
                executor.shutdown();
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (Exception e) {
                Exception finalE = abort(e);
                futureQueue.add(CompletableFuture.supplyAsync(() -> {
                    results.release();
                    return new Result<>(null, null, finalE);
                }, executor));
            } finally {
                results.release();
                futureQueue.add(POISON_PILL);
            }
        });

        return StreamSupport.stream(new Spliterator<Result<R>>() {
            private boolean poisonSeen;
            @Override
            public boolean tryAdvance(Consumer<? super Result<R>> action) {
                try {
                    CompletableFuture<Result<R>> future = null;
                    if (sort) {
                        // take the first future to keep the order
                        future = futureQueue.take();
                    } else {
                        // take the first future that is done
                        // we can loop forever because results.acquire() will block until a future is near available
                        results.acquire();
                        while (future == null) {
                            int count = 0;
                            for (CompletableFuture<Result<R>> f : futureQueue) {
                                ++count;
                                if (f == POISON_PILL) {
                                    if (count == 1) {
                                        // poison pill is the first item, so we can break as it is the end
                                        future = futureQueue.take();
                                        break;
                                    } else if (!poisonSeen){
                                        // if we've just seen the poison pill for the first time, we need to
                                        // wait for another future to be near available
                                        poisonSeen = true;
                                        results.acquire();
                                        break;
                                    }
                                }
                                else if (f.isDone()) {
                                    // any other future that is done is good
                                    future = f;
                                    break;
                                }
                            }
                        }
                    }
                    if (future == POISON_PILL) {
                        return false;
                    }
                    Result<R> result = future.get();
                    if (result.exception != null) {
                        throw new RuntimeException(result.exception);
                    }
                    action.accept(result);
                    return true;
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Spliterator<Result<R>> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                return Long.MAX_VALUE;
            }

            @Override
            public int characteristics() {
                return ORDERED | NONNULL | CONCURRENT;
            }
        }, false);
    }

    public Exception abort(Exception e) {
        if (isAborted.compareAndSet(null, e)) {
            semaphore.release(9999);
            return e;
        }
        return isAborted.get();
    }

    public interface Task<T, R> {
        R accept(T item) throws Exception;
    }

    public static class Result<R> {
        private final R result;
        private final Semaphore semaphore;
        private final Exception exception;
        private boolean released;

        public Result(R result, Semaphore semaphore, Exception exception) {
            this.result = result;
            this.semaphore = semaphore;
            this.exception = exception;
        }

        public R getResult() {
            return result;
        }

        public void release() {
            if (!released && semaphore != null) {
                semaphore.release();
                released = true;
            }
        }
    }
}