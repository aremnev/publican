package net.thumbtack.sharding.test.common;

import net.thumbtack.sharding.test.ShardingFacade;

public interface Storage {

    public void reset() throws Exception;

    public ShardingFacade sharding();
}
