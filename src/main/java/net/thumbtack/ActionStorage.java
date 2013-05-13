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

    public void addAction(Bucket bucket, Action action) {

    }

    public Date now() { // TODO impl
        return now;
    }

    public List<Action> retrieveActionsForBucketAfterDate(Bucket bucket, Date date) {
        return null;   // TODO impl
    }

    public List<Action> retrieveAllActions(Bucket bucket) {
        throw new NotImplementedException(); // TODO impl
    }
}
