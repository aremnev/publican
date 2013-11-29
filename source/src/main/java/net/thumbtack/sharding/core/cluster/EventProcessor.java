package net.thumbtack.sharding.core.cluster;

public interface EventProcessor extends EventListener {

    @Override
    void onEvent(Event event);

    void setEventListener(EventListener listener);
}
