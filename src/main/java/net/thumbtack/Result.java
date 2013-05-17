package net.thumbtack;

import java.util.List;
import java.util.Map;

public class Result {
    private Object result;
    private Map<Long, Object> entityIdEntityMap;

    public Object getResult() {
        return result;
    }

    public void setResult(List result) {
        this.result = result;
    }

    public Map<Long, Object> getEntityIdEntityMap() {
        return entityIdEntityMap;
    }

    public void setEntityIdEntityMap(Map<Long, Object> entityIdEntityMap) {
        this.entityIdEntityMap = entityIdEntityMap;
    }
}
