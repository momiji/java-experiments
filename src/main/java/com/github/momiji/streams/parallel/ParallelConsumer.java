package com.github.momiji.streams.parallel;

/**
 * Represents an operation that accepts one argument and returns no result.
 * Compared to {@link java.util.function.Consumer}, this interface throws an exception.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(Object)}.
 *
 * @param <T> the type of the input to the function
 */
@FunctionalInterface
public interface ParallelConsumer<T> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the function argument
     */
    void accept(T t) throws Exception;
}
