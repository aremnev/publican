package net.thumbtack.sharding.core;

public interface QueryClosure<V>{
    V call(Connection connection) throws Exception ;
}
