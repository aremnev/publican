package net.thumbtack.sharding.core;

import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

public class SelectAllShardsSumAsync extends QueryAsync {

    private static final Logger logger = LoggerFactory.getLogger("SelectAllShardsSumAsync");

    public SelectAllShardsSumAsync(Executor executor) {
        super(executor);
    }

    @Override
    protected <U> Object createResult() {
        return new MutableInt(0);
    }

    @Override
    protected <U> void processResult(Object result, U threadResult) {
        ((MutableInt) result).setValue(((MutableInt) result).getValue() + (Integer) threadResult);
    }

    @Override
    protected <U> boolean checkResultFinish(Object result) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> U extractResultValue(Object result) {
        return (U) ((MutableInt) result).getValue();
    }

    @Override
    protected Logger logger() {
        return logger;
    }
}
