package net.thumbtack;

import java.util.ArrayList;
import java.util.List;

public class ActionsQueueAll implements ActionsQueue {

    private List<Action> actions;

    public ActionsQueueAll(Bucket bucket) {
        actions = new ArrayList<Action>();
        // TODO get all entities from bucket and convert to Action.Insert. (use Shard depended ActionFactory.createInsertAction(Enity) ?)
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
