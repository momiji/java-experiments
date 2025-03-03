package com.github.momiji.wip;

public class SortedItem<T> {
    private final T item;
    private final SortingLocks.Lock limiter;
    private boolean acquired = false;
    private boolean released;

    public SortedItem(T item, SortingLocks.Lock limiter) {
        this.item = item;
        this.limiter = limiter;
    }

    public T get() {
        return item;
    }

    public void acquireSorted() {
        if (!acquired && limiter != null) {
            limiter.acquire();
            acquired = true;
        }
    }

    public void releaseSorted() {
        if (!released && limiter != null) {
            limiter.release();
            released = true;
        }
    }

    @Override
    public String toString() {
        return "SortedItem{" +
                "item=" + item +
                '}';
    }
}
