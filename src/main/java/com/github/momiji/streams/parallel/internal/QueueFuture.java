package com.github.momiji.streams.parallel.internal;

import java.util.concurrent.Future;

public class QueueFuture<T> extends QueueItem<T> {
    private final Future<QueueData<T>> future;

    public QueueFuture(Future<QueueData<T>> future) {
        super();
        this.future = future;
    }

    public Future<QueueData<T>> getFuture() {
        return future;
    }
}
