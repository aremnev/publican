package net.thumbtack.sharding.core.query;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Select from undefined shard. All shards are examined asynchronously.
 */
public class SelectShardAsync extends QueryAsync {

    private static final Logger logger = LoggerFactory.getLogger("SelectShardAsync");

    @Override
    protected <U> Object createResult() {
        return new AtomicReference<U>(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> void processResult(Object result, U threadResult) {
        if (threadResult != null) {
            if (threadResult instanceof Collection<?>) {
                U value = ((AtomicReference<U>) result).get();
                if (value != null) {
                    if (((Collection<?>) value).size() == 0) {
                        ((AtomicReference<U>) result).set(threadResult);
                    }
                } else {
                    ((AtomicReference<U>) result).set(threadResult);
                }
            } else {
                ((AtomicReference<U>) result).set(threadResult);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> boolean checkResultFinish(Object result) {
        U value = ((AtomicReference<U>) result).get();
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
        return ((AtomicReference<U>) result).get();
    }

    @Override
    protected Logger logger() {
        return logger;
    }
}
