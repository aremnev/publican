package net.thumbtack.sharding;

import net.thumbtack.sharding.ShardingFacade;

public interface Storage {

    public void reset() throws Exception;

    public ShardingFacade sharding();
}
