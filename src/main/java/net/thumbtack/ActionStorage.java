package net.thumbtack;

import java.util.Iterator;
import java.util.List;

public class ActionStorage {
    private static ActionStorage instance = new ActionStorage();
    private long now;

    private ActionStorage() {

    }

    public static ActionStorage getInstance() {
        return instance;
    }

    /**
     *
     * @param bucketIndex
     * @param action
     * @return actionId.
     */
    public long addAction(int bucketIndex, Action action) {
       return 0; // todo implement me.
    }

    public long now() { // TODO impl
        return now;
    }

    public List<Action> retrieveActionsForBucketAfterDate(int bucketIndex, long date) {
        return null;   // TODO impl
    }

    public Iterator<Action> findActionsBetween(long lastAcceptedActionId, long lastActionId) {
        return null;  // TODO impl
    }
}
