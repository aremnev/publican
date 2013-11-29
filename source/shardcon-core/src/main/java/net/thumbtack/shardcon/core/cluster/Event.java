package net.thumbtack.shardcon.core.cluster;

import java.io.Serializable;

public interface Event<T extends Serializable> extends Serializable {

    long getId();

    T getEventObject();
}
