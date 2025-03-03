package com.github.momiji.wip;

import manifold.ext.rt.api.Jailbreak;
import manifold.ext.rt.api.auto;

import java.util.Map;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello, World!".addExclamationMark());

        auto t = getPerson();
        System.out.println(t.name);

        @Jailbreak PrivateFunc c = new PrivateFunc();
        c.add();
        System.out.println(c.counter);

        Map<String, String> env = System.getenv();
        env.put("hello", "world");

//        List<String> strings = List.of("aa", "bb", "cc", "dd", "e", "ff", "gg", "hh", "ii", "jj");
//        strings.stream()
//                .map(String::addExclamationMark)
//                .forEach(System.out::println);
    }

    public static auto getPerson() {
        return (name: "John", age: 30);
    }
}