package net.thumbtack.sharding.common;

import net.thumbtack.sharding.core.ShardingFacade;

public interface StorageServer {

    public void start() throws Exception;

    public void stop() throws Exception;

    public void reset() throws Exception;

    public ShardingFacade sharding();
}
