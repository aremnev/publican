package net.thumbtack.sharding;

public interface Connection {

	void open();

	void commit();

	void rollback();

	void close();
}
