package com.github.momiji.wip;

import java.util.concurrent.Semaphore;

public class SortingLocks {
    private Semaphore current = new Semaphore(1);

    public SortingLocks() {
    }

    public Lock next() {
        Semaphore c = current;
        current = new Semaphore(0);
        return new Lock(c, current);
    }

    public <T> SortedItem<T> next(T item) {
        return new SortedItem<>(item, next());
    }

    public static class Lock {
        private final Semaphore current;
        private final Semaphore next;

        public Lock(Semaphore current, Semaphore next) {
            this.current = current;
            this.next = next;
        }

        public void acquire() {
            current.acquireUninterruptibly();
        }

        public void release() {
            next.release();
        }
    }
}
