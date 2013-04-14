package net.thumbtack.sharding.core;

import org.apache.commons.lang3.mutable.MutableObject;

import java.util.Collection;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectShardAsync extends QueryAsync {

    private static final Logger logger = LoggerFactory.getLogger("SelectShardAsync");

    public SelectShardAsync(Executor executor) {
        super(executor);
    }

    @Override
    protected <U> Object createResult() {
        return new MutableObject<U>(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> void processResult(Object result, U threadResult) {
        if (threadResult != null) {
            if (threadResult instanceof Collection<?>) {
                U value = ((MutableObject<U>) result).getValue();
                if (value != null) {
                    if (((Collection<?>) value).size() == 0) {
                        ((MutableObject<U>) result).setValue(threadResult);
                    }
                } else {
                    ((MutableObject<U>) result).setValue(threadResult);
                }
            } else {
                ((MutableObject<U>) result).setValue(threadResult);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> boolean checkResultFinish(Object result) {
        U value = ((MutableObject<U>) result).getValue();
        if (value != null) {
            if (value instanceof Collection<?>) {
                return ((Collection<?>) value).size() > 0;
            }
        }
        return value != null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> U extractResultValue(Object result) {
        return ((MutableObject<U>) result).getValue();
    }

    @Override
    protected Logger logger() {
        return logger;
    }
}
