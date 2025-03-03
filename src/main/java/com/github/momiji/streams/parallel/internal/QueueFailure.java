package com.github.momiji.streams.parallel.internal;

public class QueueFailure<T> implements QueueItem<T> {
    private final Exception exception;

    public QueueFailure(Exception exception) {
        super();
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }
}
