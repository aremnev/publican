package net.thumbtack.shardcon.cluster;

import java.io.Serializable;

public interface EventListener {

    void onEvent(Serializable event);
}
