package com.github.momiji.streams.parallel.internal;

import java.util.concurrent.Future;

public class QueueFuture<T> extends QueueItem<T> {
    private final Future<QueueItem<T>> future;

    public QueueFuture(Future<QueueItem<T>> future) {
        super();
        this.future = future;
    }

    public Future<QueueItem<T>> getFuture() {
        return future;
    }
}
