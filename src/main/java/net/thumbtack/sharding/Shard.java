package net.thumbtack.sharding;

public interface Shard {
	Connection getConnection();
}
