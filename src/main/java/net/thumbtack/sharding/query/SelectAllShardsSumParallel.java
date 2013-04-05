package net.thumbtack.sharding.query;

import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectAllShardsSumParallel extends QueryParallel {

	private static final Logger logger = LoggerFactory.getLogger("SelectAllShardsSumParallel");

	public SelectAllShardsSumParallel(QueryEngine engine) {
		super(engine);
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
