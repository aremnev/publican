package net.thumbtack.shardcon;

public interface Storage {

    public void reset() throws Exception;

    public ShardingFacade sharding();
}
