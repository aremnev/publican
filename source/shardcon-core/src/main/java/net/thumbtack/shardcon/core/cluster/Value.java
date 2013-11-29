package net.thumbtack.shardcon.core.cluster;

public interface Value<T> {

    T get();

    void set(T value);
}
