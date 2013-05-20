package net.thumbtack;

import net.thumbtack.sharding.core.query.Connection;

import java.util.Date;

abstract public class Action { // looks like QueryClosure...
    private Long actionId;
    private ActionType actionType;

    public Long getActionId() {
        return actionId;
    }

    public ActionType getActionType() {
        return actionType;
    }

    abstract Result call(Connection connection) throws ActionException;
}
