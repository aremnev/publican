package net.thumbtack.sharding.query;

public interface QueryClosure<V>{
	V call(Connection connection);
}
