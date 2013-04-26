package net.thumbtack.sharding.example.memcached;

import java.io.Serializable;

public class User implements Serializable {
    private String name;
    private int age;

    public int getAge() {
        return age;
    }

    public String getName() {
        return name;
    }

    public User(String name, int age) {
        this.name = name;
        this.age = age;
    }
}
