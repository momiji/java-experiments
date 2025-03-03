package com.github.momiji.streams.parallel;

/**
 * Represents a function that accepts one argument and produces a result.
 * Compared to {@link java.util.function.Function}, this interface throws an exception.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #apply(Object)}.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
public interface ParallelFunction<T, R> {

    /**
     * Applies this function to the given argument.
     *
     * @param t the function argument
     * @return the function result
     */
    R apply(T t) throws Exception;
}
