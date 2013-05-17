package net.thumbtack;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Date;
import java.util.List;

public class ActionStorage {
    private static ActionStorage instance = new ActionStorage();
    private Date now;

    private ActionStorage() {

    }

    public static ActionStorage getInstance() {
        return instance;
    }

    public void addAction(int bucketIndex, Action action) {

    }

    public Date now() { // TODO impl
        return now;
    }

    public List<Action> retrieveActionsForBucketAfterDate(int bucketIndex, Date date) {
        return null;   // TODO impl
    }
}
