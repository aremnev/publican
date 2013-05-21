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
     * add action to ahead log.
     * @param bucketIndex
     * @param action
     * @return actionId.
     */
    public long addAction(int bucketIndex, Action action) {
       return 0; // todo implement me.
    }

    /**
     * retrieve data from ahead log.
     * @param date
     * @return
     */
    public List<Action> retrieveActionsAfter(long date) {
        return null;   // TODO impl
    }

    /**
     * retrieve data from ahead log.
     * @param lastAcceptedActionId
     * @param lastActionId
     * @return
     */
    public Iterator<Action> findActionsBetween(long lastAcceptedActionId, long lastActionId) {
        return null;  // TODO impl
    }
}
