package net.thumbtack.sharding.core;

public interface Configuration {

	long getSelectAnyRandomSeed();

	int getNumberOfWorkerThreads();

	Iterable<Shard> getShards();

	KeyMapper getKeyMapper();
}
