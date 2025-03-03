package com.github.momiji.streams.parallel.internal;

public class QueueException<T> extends QueueItem<T> {
    private final Exception exception;

    public QueueException(Exception exception) {
        super();
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }
}
