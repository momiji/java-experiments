package com.github.momiji.streams.parallel;

public class ParallelException extends RuntimeException {
    public ParallelException(Exception e) {
        super(e);
    }

    public ParallelException(String message) {
        super(message);
    }
}
