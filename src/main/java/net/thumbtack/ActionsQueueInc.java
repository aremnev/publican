package net.thumbtack;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class ActionsQueueInc implements ActionsQueue {
    private long date;
    private Bucket bucket;
    private List<Action> actions = new LinkedList<Action>();

    public ActionsQueueInc(Bucket bucket) {
        this.bucket = bucket;
        date = ActionStorage.getInstance().now();
    }

    private void update() {
        List<Action> actionsInc = ActionStorage.getInstance().retrieveActionsForBucketAfterDate(bucket.getBucketIndex(), date);
        actions.addAll(actionsInc);
//        merge(actions);
        date = findLatestActionTime(actionsInc);
    }

    private long findLatestActionTime(List<Action> actions) {
        Long max = null;
        for (Action action : actions) {
            if (max == null) {
                max = action.getActionId();
            } else {
                if (action.getActionId() > max) {
                    max = action.getActionId();
                }
            }
        }
        return max;
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
