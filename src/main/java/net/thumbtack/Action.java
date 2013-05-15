package net.thumbtack;

import net.thumbtack.sharding.core.query.Connection;

import java.util.Date;

abstract public class Action { // looks like QueryClosure...
    private Date actionTime;
    private ActionType actionType;

    public Date getActionTime() {
        return actionTime;
    }

    public ActionType getActionType() {
        return actionType;
    }

    abstract Result call(Connection connection) throws ActionException;
}
