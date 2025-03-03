package com.github.momiji.streams.parallel;

import java.util.concurrent.Semaphore;

public class LimitedItem<T>  {
    private final T item;
    private final Semaphore limiter;
    private boolean released;

    public LimitedItem(T item, Semaphore limiter) {
        this.item = item;
        this.limiter = limiter;
    }

    public T get() {
        return item;
    }

    public void release() {
        if (!released && limiter != null) {
            limiter.release();
            released = true;
        }
    }
}
