package net.thumbtack;

import static net.thumbtack.Utils.retrieveBucketState;

public class ActionsQueueCombo implements ActionsQueue {
    private ActionsQueue actionsQueueInc;
    private ActionsQueue actionsQueueAll;
    private Bucket bucket;

    public ActionsQueueCombo(Bucket bucket) {
        actionsQueueInc = new ActionsQueueInc(bucket);
        actionsQueueAll = new ActionsQueueAll(bucket);
    }

    @Override
    public Action pop() {
        Action result;
        while (true) {
            if ((count() == 0) && (retrieveBucketState(bucket) == BucketState.D) && (Utils.retrieveBucketUsageCount(bucket) == 0)) {
                result = null;
                break;
            }
            if (actionsQueueAll.count() > 0) {
                result =  actionsQueueAll.pop();
            } else {
                result = actionsQueueInc.pop();
            }
            if (result != null) {
                break;
            }
            // sleep();
        }
        return result;
    }

    @Override
    public int count() {
        return actionsQueueInc.count() + actionsQueueAll.count();
    }
}
