package net.thumbtack;


public class ActionsQueueCombo implements ActionsQueue {
    private ActionsQueue actionsQueueInc;
    private ActionsQueue actionsQueueAll;

    public ActionsQueueCombo(Bucket bucket) {
        actionsQueueInc = new ActionsQueueInc(bucket);
        actionsQueueAll = new ActionsQueueAll(bucket);
    }

    @Override
    public Action pop() throws ActionsQueueException {
        Action result = null;
        if (!actionsQueueAll.isEmpty()) {
            result = actionsQueueAll.pop();
        } else {
            if (!actionsQueueInc.isEmpty()) {
                result = actionsQueueInc.pop();
            }
        }
        return result;
    }

    @Override
    public int count() {
        return actionsQueueInc.count() + actionsQueueAll.count();
    }

    @Override
    public boolean isEmpty() {
        return actionsQueueInc.isEmpty() && actionsQueueAll.isEmpty();
    }
}
