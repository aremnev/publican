package net.thumbtack.shardcon.cluster;

import java.io.Serializable;

public interface Event<T extends Serializable> extends Serializable {

    long getId();

    T getEventObject();
}
