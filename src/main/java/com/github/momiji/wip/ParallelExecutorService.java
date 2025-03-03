package com.github.momiji.wip;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ParallelExecutorService<I, O, R> {

    private final ExecutorService executor;
    private final Action<I, O> parallel;
    private final Action<O, R> sequential;
    private final ConcurrentLinkedQueue<Result<O>> queue = new ConcurrentLinkedQueue<>();
    private final Result<O> POISON_PILL = new Result<>(null, null);
    private final AtomicReference<Exception> exception = new AtomicReference<>(null);

    public ParallelExecutorService(int nThreads, Action<I, O> parallel, Action<O, R> sequential) {
        this.executor = Executors.newFixedThreadPool(nThreads);
        this.parallel = parallel;
        this.sequential = sequential;
    }

    public void submit(I item) {
        executor.submit(() -> {
            try {
                if (exception.get() != null) {
                    return;
                }
                queue.add(new Result(parallel.accept(item), null));
            } catch (Exception e) {
                e = abort(e);
                queue.add(new Result(null, e));
            } finally {
                queue.add(POISON_PILL);
            }
        });
    }

    private Exception abort(Exception e) {
        if (exception.compareAndSet(null, e)) {
            aborted(e);
            return e;
        }
        return exception.get();
    }

    protected void aborted(Exception e) {
    }

    public interface Action<I, O> {
        O accept(I item) throws Exception;
    }

    public static class Result<O> {
        private final O result;
        private final Exception exception;

        public Result(O result, Exception exception) {
            this.result = result;
            this.exception = exception;
        }

        public O getResult() {
            return result;
        }

        public Exception getException() {
            return exception;
        }
    }
}
