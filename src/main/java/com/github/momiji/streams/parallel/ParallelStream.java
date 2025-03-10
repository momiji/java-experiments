package com.github.momiji.streams.parallel;

import com.github.momiji.streams.parallel.internal.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ParallelStream<T> {

    private Stream<T> input;
    private ParallelConfig config;
    private final BlockingQueue<QueueItem<T>> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean aborted = new AtomicBoolean(false);

    private ExecutorService executor;
    private Semaphore queueSemaphore;
    private boolean sorted;

    private ParallelStream() {
    }

    public static <T> ParallelStream<T> of(Stream<T> input) {
        ParallelStream<T> ps = new ParallelStream<>();
        ps.input = input;
        return ps;
    }

    private void init() {
        if (config == null) {
            config = ParallelConfig.newConfig()
                    .setNThreads(Runtime.getRuntime().availableProcessors())
                    .setQueueSize(100);
        }
        if (config.getNThreads() > 1) {
            executor = Executors.newFixedThreadPool(config.getNThreads());
        } else {
            executor = Executors.newSingleThreadExecutor();
        }
        queueSemaphore = new Semaphore(config.getQueueSize());
    }

    private Iterator<T> iter() {
        if (input == null) return Collections.emptyIterator();
        return input.iterator();
    }

    //

    public ParallelStream<T> executor(int nThreads, int queueSize) {
        if (nThreads < 1) {
            throw new IllegalArgumentException("nThreads must be greater than 0");
        }
        if (queueSize < 1) {
            throw new IllegalArgumentException("queueSize must be greater than 0");
        }
        if (nThreads > queueSize) {
            throw new IllegalArgumentException("nThreads must be less than or equal to queueSize");
        }
        config = ParallelConfig.newConfig().setNThreads(nThreads).setQueueSize(queueSize);
        executor = Executors.newFixedThreadPool(nThreads);
        queueSemaphore = new Semaphore(queueSize);
        return this;
    }

    //

    public void run() {
        Iterator<T> it = iter();
        while (it.hasNext()) {
            it.next();
        }
    }

    public Stream<T> stream() {
        return input;
    }

    public void forEach(ParallelConsumer<? super T> action) {
        input.forEach(e -> {
            try {
                action.accept(e);
            } catch (Exception ex) {
                throw new ParallelException(ex);
            }
        });
    }

    public <R> ParallelStream<R> map(ParallelFunction<? super T, ? extends R> mapper) {
        ParallelStream<R> r = new ParallelStream<>();
        r.config = config;
        r.sorted = sorted;
        r.input = input.map(e -> {
            try {
                return mapper.apply(e);
            } catch (Exception ex) {
                throw new ParallelException(ex);
            }
        });
        return r;
    }

    //

    public void parallelForEach(ParallelConsumer<? super T> action) {
        parallelMap(e -> {
            action.accept(e);
            return e;
        }).run();
    }

    public <R> ParallelStream<R> parallelMap(ParallelFunction<? super T, ? extends R> mapper) {
        ParallelStream<R> r = new ParallelStream<>();
        //
        init();
        CompletableFuture.runAsync(() -> {
            try {
                long count = 0;
                Iterator<T> it = iter();
                while (it.hasNext() && !aborted.get()) {
                    count++;
                    T item = it.next();
                    Future<QueueItem<R>> future = executor.submit(() -> {
                        try {
                            if (aborted.get()) return null;
                            queueSemaphore.acquire();
                            if (aborted.get()) return null;
                            QueueData<R> data = new QueueData<>(mapper.apply(item), queueSemaphore);
                            if (aborted.get()) return null;
                            if (!sorted) r.queue.add(data);
                            return data;
                        } catch (Exception ex) {
                            aborted.set(true);
                            if (ex instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                            QueueFailure<R> data = new QueueFailure<>(ex);
                            if (!sorted) r.queue.add(data);
                            return data;
                        }
                    });
                    if (sorted) r.queue.add(new QueueFuture<>(future));
                }
                r.queue.add(new QueueSize<>(count));
            } catch (Exception ex) {
                aborted.set(true);
                r.queue.add(new QueueFailure<>(ex));
            } finally {
                executor.shutdown();
            }
        });
        r.config = config;
        r.input = StreamSupport.stream(new QueueSpliterator<>(r.queue, aborted), false);
        return r;
    }

    //

    /**
     * Limits the number of concurrent operations using a semaphore.
     * <br><br>
     * This method wraps each item in the stream with a {@link LimitedItem} that contains
     * a semaphore to control the concurrency. The semaphore is initialized with
     * the limit parameter.
     * <br><br>
     * Use {@link LimitedItem#release()}  method to allow more items to be processed.
     *
     * @return a new `ParallelStream` where each item is wrapped in a `LimitedItem`
     *         that uses a semaphore to limit concurrency.
     */
    public ParallelStream<LimitedItem<T>> limited(int limit) {
        Semaphore limiter = new Semaphore(limit);
        return map(e -> {
            limiter.acquire();
            return new LimitedItem<>(e, limiter);
        });
    }

    public ParallelStream<T> sorted() {
        sorted = true;
        return this;
    }
}
