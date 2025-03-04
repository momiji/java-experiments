package com.github.momiji.streams.parallel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Execution(ExecutionMode.CONCURRENT)
class ParallelStreamTest {

    Stream<Integer> values() {
        return Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
    }

    List<Integer> list() {
        return values().collect(Collectors.toList());
    }

    @Test
    void run1() {
        assertDoesNotThrow(() -> {
            ParallelStream.of(values()).run();
        });
    }

    @Test
    void run2() {
        assertDoesNotThrow(() -> {
            ParallelStream.of(values()).executor(4, 100).run();
        });
    }

    @Test
    void run3() {
        Queue<Integer> res = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(4, 100)
                .parallelMap(e -> {
                    Thread.sleep((long) (Math.random() * 1000));
                    System.out.println("> " + e);
                    res.add(e);
                    return e;
                })
                .run();
        //
        assertEquals(list(), res.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void stream() {
        Queue<Integer> res = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(4, 100)
                .parallelMap(e -> {
                    Thread.sleep((long) (Math.random() * 1000));
                    return e;
                })
                .stream()
                .forEach(e -> {
                    System.out.println("> " + e);
                    res.add(e);
                });
        //
        assertEquals(list(), res.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void forEach() {
        Queue<Integer> res = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(4, 100)
                .forEach(e -> {
                    System.out.println("> " + e);
                    res.add(e);
                });
        //
        assertEquals(list(), res.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void forEachException() {
        Queue<Integer> res = new ConcurrentLinkedQueue<>();
        //
        assertThrows(ParallelException.class, () -> {
            ParallelStream.of(values())
                    .executor(4, 100)
                    .forEach(e -> {
                        System.out.println("> " + e);
                        if (e == 5) throw new Exception("oops");
                        res.add(e);
                    });
        });
        //
        assertEquals(values().limit(4).collect(Collectors.toList()), res.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void map() {
        Queue<Integer> res = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(4, 100)
                .map(e -> {
                    System.out.println("> " + e);
                    res.add(e);
                    return e;
                })
                .run();
        //
        assertEquals(list(), res.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void mapException() {
        Queue<Integer> res = new ConcurrentLinkedQueue<>();
        //
        assertThrows(ParallelException.class, () -> {
            ParallelStream.of(values())
                    .executor(4, 100)
                    .map(e -> {
                        System.out.println("> " + e);
                        if (e == 5) throw new Exception("oops");
                        res.add(e);
                        return e;
                    })
                    .run();
        });
        //
        assertEquals(values().limit(4).collect(Collectors.toList()), res.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void parallelForEach() {
        Queue<Integer> res = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(4, 100)
                .parallelForEach(e -> {
                    Thread.sleep((long) (Math.random() * 1000));
                    System.out.println("> " + e);
                    res.add(e);
                });
        //
        assertEquals(list(), res.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void limited() {
        Queue<String> res = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(2, 4)
                .limited()
                .parallelMap(e -> {
                    System.out.println("> " + e.get());
                    Thread.sleep((long) (Math.random() * 100));
                    res.add("+");
                    return e;
                })
                .executor(2,2)
                .parallelMap(e -> {
                    Thread.sleep((long) (500 + Math.random() * 500));
                    System.out.println("==> " + e.get());
                    res.add("-");
                    e.release();
                    return e;
                })
                .run();
        //
        System.out.println(String.join("", res));
        assertFalse(String.join("", res).contains("+++++"));
    }

    /**
     * Ensure than sorted() works even if acquire/release is not called.
     * Also verify that all items are going to the queue before being stream to the next step,
     * as we're explicitly slowing down the processing of the first item.
     */
    @Test
    void sorted1() {
        Queue<String> res1 = new ConcurrentLinkedQueue<>();
        Queue<Integer> res2 = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(4, 100)
                .sorted()
                .parallelMap(e -> {
                    System.out.println("> " + e);
                    res1.add("+");
                    if (e==1) Thread.sleep(1000);
                    return e;
                })
                .map(e -> {
                    res1.add("-");
                    res2.add(e);
                    System.out.println("====> " + e);
                    return e;
                })
                .run();
        //
        System.out.println(String.join("", res1));
        assertEquals("++++++++++++++++++++--------------------", String.join("", res1));
        assertEquals(values().collect(Collectors.toList()), new ArrayList<>(res2));
    }

    /**
     * Ensure that a second parallel is unsorted.
     */
    @Test
    void sorted2() {
        Queue<String> res1 = new ConcurrentLinkedQueue<>();
        Queue<Integer> res2 = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(4, 100)
                .sorted()
                .parallelMap(e -> {
                    if (e==1) Thread.sleep(1000);
                    return e;
                })
                .parallelMap(e -> {
                    res1.add("+");
                    System.out.println("> " + e);
                    if (e==1) Thread.sleep(1000);
                    return e;
                })
                .map(e -> {
                    res1.add("-");
                    res2.add(e);
                    System.out.println("==> " + e);
                    return e;
                })
                .run();
        //
        System.out.println(String.join("", res1));
        System.out.println(Arrays.toString(new ArrayList<>(res2).toArray()));
        assertNotEquals("++++++++++++++++++++--------------------", String.join("", res1));
        assertNotEquals(values().collect(Collectors.toList()), new ArrayList<>(res2));
    }

    /**
     * Ensure that multiple sorted() works
     */
    @Test
    void sorted3() {
        Queue<Integer> res1 = new ConcurrentLinkedQueue<>();
        Queue<Integer> res2 = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(4, 100)
                .sorted()
                .parallelMap(e -> {
                    if (e==1) Thread.sleep(1000);
                    else if (e % 2 == 0) Thread.sleep(500);
                    System.out.println("> " + e);
                    return e;
                })
                .map(e -> {
                    res1.add(e);
                    System.out.println("==> " + e);
                    return e;
                })
                .sorted()
                .parallelMap(e -> {
                    System.out.println(">> " + e);
                    if (e==1) Thread.sleep(100);
                    return e;
                })
                .map(e -> {
                    res2.add(e);
                    System.out.println("====>> " + e);
                    return e;
                })
                .run();
        //
        assertEquals(values().collect(Collectors.toList()), new ArrayList<>(res1));
        assertEquals(values().collect(Collectors.toList()), new ArrayList<>(res2));
    }

    /**
     * Test with limited() then sorted().
     */
    @Test
    void limitedAndSorted1() {
        Queue<String> res1 = new ConcurrentLinkedQueue<>();
        Queue<Integer> res2 = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(2, 4)
                .limited()
                .sorted()
                .parallelMap(e -> {
                    System.out.println("> " + e.get());
                    res1.add("+");
                    if (e.get()==1) Thread.sleep(1000);
                    return e;
                })
                .map(e -> {
                    res1.add("-");
                    res2.add(e.get());
                    System.out.println("====> " + e.get());
                    Thread.sleep((long) (Math.random() * 100));
                    e.release();
                    return e;
                })
                .run();
        //
        System.out.println(String.join("", res1));
        assertFalse(String.join("", res1).contains("+++++"));
        assertEquals(values().collect(Collectors.toList()), new ArrayList<>(res2));
    }

    /**
     * Test with sorted() then limited().
     */
    @Test
    void sortedAndLimited() {
        Queue<String> res1 = new ConcurrentLinkedQueue<>();
        Queue<Integer> res2 = new ConcurrentLinkedQueue<>();
        //
        ParallelStream.of(values())
                .executor(2, 4)
                .sorted()
                .limited()
                .parallelMap(e -> {
                    System.out.println("> " + e.get());
                    res1.add("+");
                    if (e.get()==1) Thread.sleep(1000);
                    return e;
                })
                .map(e -> {
                    res1.add("-");
                    res2.add(e.get());
                    System.out.println("====> " + e.get());
                    Thread.sleep((long) (Math.random() * 100));
                    e.release();
                    return e;
                })
                .run();
        //
        System.out.println(String.join("", res1));
        assertFalse(String.join("", res1).contains("+++++"));
        assertEquals(values().collect(Collectors.toList()), new ArrayList<>(res2));
    }

    @Test
    void exception1() {
        assertThrows(ParallelException.class, () -> {
            ParallelStream.of(values())
                    .executor(4, 100)
                    .parallelMap(e -> {
                        System.out.println("> " + e);
                        Thread.sleep((long) (Math.random() * 1000));
                        System.out.println("==> " + e);
                        if (e == 5) throw new Exception("oops");
                        return e;
                    })
                    .run();
        });
    }

    @Test
    void exception2() {
        assertThrows(ParallelException.class, () -> {
            ParallelStream.of(values())
                    .executor(4, 100)
                    .parallelMap(e -> {
                        System.out.println("> " + e);
                        Thread.sleep((long) (500 + Math.random() * 500));
                        System.out.println("==> " + e);
                        if (e == 5) throw new Exception("oops");
                        return e;
                    })
                    .parallelMap(e -> {
                        System.out.println("====> " + e);
                        return e;
                    })
                    .run();
        });
    }

    @Test
    void exception3() {
        assertThrows(ParallelException.class, () -> {
            ParallelStream.of(values())
                    .executor(4, 100)
                    .parallelMap(e -> {
                        System.out.println("> " + e);
                        Thread.sleep((long) (500 + Math.random() * 500));
                        System.out.println("==> " + e);
                        return e;
                    })
                    .parallelMap(e -> {
                        System.out.println("====> " + e);
                        if (e == 5) throw new Exception("oops");
                        return e;
                    })
                    .run();
        });
    }
}