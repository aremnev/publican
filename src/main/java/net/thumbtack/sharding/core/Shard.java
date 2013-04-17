package net.thumbtack.sharding.core;

import net.thumbtack.sharding.core.query.Connection;

public interface Shard {

    long getId();

    Connection getConnection();
}
