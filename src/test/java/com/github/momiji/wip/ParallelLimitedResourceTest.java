package com.github.momiji.wip;

import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

class ParallelLimitedResourceTest {

    @Test
    void process() throws InterruptedException {
        // create a new stream of 10 integers
        Stream<Integer> stream = Stream.iterate(0, i -> i + 1).limit(20);

        Stream<ParallelLimitedResource.Result<Integer>> downloads = new ParallelLimitedResource<>(stream, 4, true, i -> {
            log("==> Start download " + i);
            // maxSleep(1000);
            if (i == 5) {
                // throw new RuntimeException("Download failed");
            }
            log("Stop download " + i);
            return i;
        }).process();

        Stream<ParallelLimitedResource.Result<Integer>> uploads = new ParallelLimitedResource<>(downloads, 2, true, r -> {
            log("Start upload " + r.getResult());
            // maxSleep(1000);
            log("==> Stop upload " + r.getResult());
            r.release();
            return r.getResult();
        }).process();

        uploads.forEach(r -> r.release());
    }

    private static void maxSleep(int millis) {
        try {
            Thread.sleep((long) (Math.random() * millis));
        } catch (InterruptedException e) {
            // forget
        }
    }

    synchronized private static void log(String s) {
        System.out.printf("%s%n", s);
    }
}