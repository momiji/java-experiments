package com.github.momiji.streams.parallel.internal;

import com.github.momiji.streams.parallel.ParallelException;

import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class QueueSpliterator<T> implements Spliterator<T> {
    private final BlockingQueue<QueueItem<T>> queue;
    private final AtomicBoolean aborted;
    private long count = 0;
    private long size = -1;

    public QueueSpliterator(BlockingQueue<QueueItem<T>> queue, AtomicBoolean aborted) {
        this.queue = queue;
        this.aborted = aborted;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        try {
            for (;;) {
                if (size != -1 && count == size) {
                    return false;
                }
                QueueItem<T> item = queue.take();
                // if future, use result
                if (item instanceof QueueFuture) {
                    QueueFuture<T> qFuture = (QueueFuture<T>) item;
                    item = qFuture.getFuture().get();
                }
                // process item
                if (item instanceof QueueData) {
                    QueueData<T> qData = (QueueData<T>) item;
                    count++;
                    action.accept(qData.getValue());
                    qData.getSemaphore().release();
                    return true;
                }
                if (item instanceof QueueSize) {
                    QueueSize<T> qSize = (QueueSize<T>) item;
                    size = qSize.getSize();
                }
                if (item instanceof QueueException) {
                    QueueException<T> qException = (QueueException<T>) item;
                    throw qException.getException();
                }
            }
        } catch (RuntimeException ex) {
            aborted.set(true);
            throw ex;
        } catch (Exception ex) {
            aborted.set(true);
            throw new ParallelException(ex);
        }
    }

    @Override
    public Spliterator<T> trySplit() {
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
}
