package net.thumbtack.shardcon.core.cluster;

public interface EventProcessor extends EventListener {

    @Override
    void onEvent(Event event);

    void setEventListener(EventListener listener);
}
