package com.github.momiji.streams.parallel.internal;

public class ParallelConfig {
    private int nThreads;
    private int queueSize;

    private ParallelConfig() {
    }

    public static ParallelConfig newConfig() {
        return new ParallelConfig();
    }

    public int getQueueSize() {
        return queueSize;
    }

    public ParallelConfig setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    public int getNThreads() {
        return nThreads;
    }

    public ParallelConfig setNThreads(int nThreads) {
        this.nThreads = nThreads;
        return this;
    }
}
