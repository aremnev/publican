package net.thumbtack;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

public class ActionsQueueAll implements ActionsQueue {

    private List<Action> actions;

    public ActionsQueueAll(Bucket bucket) {
        actions =  retrieveAllActions(bucket);
    }

    private List<Action> retrieveAllActions(Bucket bucket) {
        throw new NotImplementedException(); // TODO
    }

    @Override
    public Action pop() {
        return actions.remove(0);
    }

    @Override
    public int count() {
        return actions.size();
    }
}
