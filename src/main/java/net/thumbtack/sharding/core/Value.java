package net.thumbtack.sharding.core;

public interface Value<T> {

    T get();

    void set(T value);
}
