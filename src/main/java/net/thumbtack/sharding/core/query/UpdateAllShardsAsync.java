package net.thumbtack.sharding.core.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Update on all shards asynchronously.
 */
public class UpdateAllShardsAsync extends QueryAsync {

    private static final Logger logger = LoggerFactory.getLogger("UpdateAllShardsAsync");

    @Override
    public boolean isUpdate() {
        return true;
    }

    @Override
    protected <U> void logErrors(List<QueryError> errors, U resultValue) {
        if (!errors.isEmpty()) {
            logger.error("SHARDS CAN BE OUT OF SYNC. See further logged error.");
        }
        super.logErrors(errors, resultValue);
    }

    @Override
    protected <U> Object createResult() {
        return new AtomicReference<U>(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> void processResult(Object result, U threadResult) {
        if (threadResult != null) {
            ((AtomicReference<U>) result).set(threadResult);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    @Override
    protected <U> boolean checkResultFinish(Object result) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> U extractResultValue(Object result) {
        return ((AtomicReference<U>) result).get();
    }

    @Override
    protected Logger logger() {
        return logger;
    }
}
