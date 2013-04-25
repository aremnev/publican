package net.thumbtack.sharding.core.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Select from all shards asynchronously with union of results to list.
 */
public class SelectAllShardsAsync extends QueryAsync {

    private static final Logger logger = LoggerFactory.getLogger("SelectAllShardsAsync");

    @Override
    protected <U> Object createResult() {
        return new ArrayList<Object>();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> void processResult(Object result, U threadResult) {
        if (threadResult != null) {
            ((Collection<Object>) result).addAll((Collection<Object>) threadResult);
        }
    }

    @Override
    protected <U> boolean checkResultFinish(Object result) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <U> U extractResultValue(Object result) {
        return (U) result;
    }

    @Override
    protected Logger logger() {
        return logger;
    }
}
