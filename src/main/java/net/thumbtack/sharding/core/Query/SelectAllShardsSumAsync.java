package net.thumbtack.sharding.core.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Select from all shards asynchronously with union of results to list.
 */
public class SelectAllShardsSumAsync extends QueryAsync {

    private static final Logger logger = LoggerFactory.getLogger("SelectAllShardsSumAsync");

    @SuppressWarnings("UnusedDeclaration")
    @Override
    protected <U> Object createResult() {
        return new AtomicLong(0);
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Override
    protected <U> void processResult(final Object result, U threadResult) {
        ((AtomicLong) result).set(((AtomicLong) result).get() + (Integer) threadResult);
    }

    @SuppressWarnings("UnusedDeclaration")
    @Override
    protected <U> boolean checkResultFinish(Object result) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> U extractResultValue(Object result) {
        return (U) Long.valueOf(((AtomicLong) result).get());
    }

    @Override
    protected Logger logger() {
        return logger;
    }
}
