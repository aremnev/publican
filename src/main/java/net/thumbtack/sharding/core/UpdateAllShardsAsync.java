package net.thumbtack.sharding.core;

import org.apache.commons.lang3.mutable.MutableObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;

public class UpdateAllShardsAsync extends QueryAsync {

    private static final Logger logger = LoggerFactory.getLogger("UpdateAllShardsAsync");

    public UpdateAllShardsAsync(Executor executor) {
        super(executor);
    }

    @Override
    protected <U> void logErrors(List<QueryError> errors, U resultValue) {
        if (!errors.isEmpty()) {
            logger.error("SHARDS CAN BE OUT OF SYNC. See further logged error.");
        }
        super.logErrors(errors, resultValue);
    }

    @Override
    protected boolean doCommit() {
        return true;
    }

    @Override
    protected <U> Object createResult() {
        return new MutableObject<U>(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> void processResult(Object result, U threadResult) {
        if (threadResult != null) {
            ((MutableObject<U>) result).setValue(threadResult);
        }
    }

    @Override
    protected <U> boolean checkResultFinish(Object result) {
        return false;
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