package net.thumbtack.sharding;

public interface Cacheable {

	void invalidateCache();

	void setCacheDisabled(boolean disabled);

	boolean getCacheDisabled();
}