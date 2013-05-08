package net.thumbtack;

import java.util.Date;

public class Action {
    private Date actionTime;
    private ActionType actionType;
    private Long entityId;
    private String serializedEntity;

    public Date getActionTime() {
        return actionTime;
    }

    public ActionType getActionType() {
        return actionType;
    }

    public Long getEntityId() {
        return entityId;
    }

    public String getSerializedEntity() {
        return serializedEntity;
    }
}
