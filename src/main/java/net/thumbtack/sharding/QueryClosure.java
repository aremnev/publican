package net.thumbtack.sharding;

public interface QueryClosure<V>{
	V call(Connection connection);
}
