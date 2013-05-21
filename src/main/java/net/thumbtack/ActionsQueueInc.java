package net.thumbtack;

import java.util.LinkedList;
import java.util.List;

public class ActionsQueueInc implements ActionsQueue {
    private long lastActionId;
    private LinkedList<Action> actions = new LinkedList<Action>();
    private BucketService bucketService;


    public ActionsQueueInc(Bucket bucket) throws BucketServiceException {
        lastActionId = bucketService.retrieveLastAcceptedActionId(bucket);
    }

    private void update() {
        List<Action> actionsInc = ActionStorage.getInstance().retrieveActionsAfter(lastActionId);
        actions.addAll(actionsInc);
//        merge(actions);
        lastActionId = actions.getLast().getActionId();
    }

    @Override
    public Action pop() {
        update();
        return actions.remove(0);
    }

    @Override
    public int count() {
        update();
        return actions.size();
    }

    @Override
    public boolean isEmpty() {
        return count() == 0;
    }
}
