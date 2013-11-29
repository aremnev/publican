package net.thumbtack.sharding.core.cluster;

public interface Value<T> {

    T get();

    void set(T value);
}
