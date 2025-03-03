package com.github.momiji.streams.parallel.internal;

public class QueueSize<T> implements QueueItem<T> {
    private final long size;

    public QueueSize(long size) {
        super();
        this.size = size;
    }

    public long getSize() {
        return size;
    }
}
