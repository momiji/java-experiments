package com.github.momiji.streams.parallel.internal;

import java.util.concurrent.Semaphore;

public class QueueData<T> implements QueueItem<T> {
    private final Semaphore semaphore;
    private final T value;

    public QueueData(T value, Semaphore semaphore) {
        super();
        this.semaphore = semaphore;
        this.value = value;
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }

    public T getValue() {
        return value;
    }
}
