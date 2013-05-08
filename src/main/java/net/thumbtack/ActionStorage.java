package net.thumbtack;

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

    public Date getNow() { // TODO impl
        return now;
    }

    public List<Action> retrieveActionsAfterDate(Date date) {
        return null;   // TODO impl
    }
}
