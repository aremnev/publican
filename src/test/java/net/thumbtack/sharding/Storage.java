package net.thumbtack.sharding;

public interface Storage {

    public void reset() throws Exception;

    public ShardingFacade sharding();
}
