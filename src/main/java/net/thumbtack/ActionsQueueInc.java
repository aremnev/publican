package net.thumbtack;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class ActionsQueueInc implements ActionsQueue {
    private Date date;
    private Bucket bucket;
    private List<Action> actions = new LinkedList<Action>();

    public ActionsQueueInc(Bucket bucket) {
        this.bucket = bucket;
        date = ActionStorage.getInstance().now();
    }

    private void update() {
        List<Action> actionsInc = ActionStorage.getInstance().retrieveActionsForBucketAfterDate(bucket, date);
        actions.addAll(actionsInc);
//        merge(actions);
        date = findLatestActionTime(actionsInc);
    }

    private Date findLatestActionTime(List<Action> actions) {
        Date max = null;
        for (Action action : actions) {
            if (max == null) {
                max = action.getActionTime();
            } else {
                if (action.getActionTime().after(max)) {
                    max = action.getActionTime();
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
