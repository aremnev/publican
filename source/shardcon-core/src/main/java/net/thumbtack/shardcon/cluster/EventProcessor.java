package net.thumbtack.shardcon.cluster;

import java.io.Serializable;

public interface EventProcessor extends EventListener {

    @Override
    void onEvent(Serializable event);

    void setEventListener(EventListener listener);
}
