package net.thumbtack;

import java.util.List;

public class ActionsQueueAll implements ActionsQueue {

    private List<Action> actions;

    public ActionsQueueAll(Bucket bucket) {
        actions =  ActionStorage.getInstance().retrieveAllActions(bucket);
    }

    @Override
    public Action pop() {
        return actions.remove(0);
    }

    @Override
    public int count() {
        return actions.size();
    }

    @Override
    public boolean isEmpty() {
        return count() == 0;
    }
}
