package net.thumbtack.sharding;

import net.thumbtack.sharding.QueryEngine;
import net.thumbtack.sharding.QueryParallel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SelectAllShardsParallel extends QueryParallel {

	private static final Logger logger = LoggerFactory.getLogger("SelectAllShardsParallel");

	public SelectAllShardsParallel(QueryEngine engine) {
		super(engine);
	}

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
