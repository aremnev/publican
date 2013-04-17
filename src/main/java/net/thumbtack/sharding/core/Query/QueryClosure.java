package net.thumbtack.sharding.core.query;

public interface QueryClosure<V> {

    V call(Connection connection) throws Exception;
}
