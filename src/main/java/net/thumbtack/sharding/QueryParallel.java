package net.thumbtack.sharding;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class QueryParallel extends Query {

	public QueryParallel(QueryEngine engine) {
		super(engine);
	}

	@Override
	public <U> U query(final QueryClosure<U> closure) {
		final Lock lock = new ReentrantLock();
		final Condition condition = lock.newCondition();
		// counter of threads that already executed
		final MutableInt threadsCount = new MutableInt(engine.shardsKeys().size());
		final Object result = this.<U>createResult();
		// all errors are accumulated here
		final List<QueryError> errors = Collections.synchronizedList(new ArrayList<QueryError>());

		// for each shard one thread is run
		// threads are synchronized by lock
		// when thread has done it send condition.signal();
		for (final int shardKey : engine.shardsKeys()) {
			// we can't do it too fast since db (or MyBatis) returns null by unclear reason
			// so send thread to sleep 1 ms
			try {
				Thread.sleep(1);
			} catch (InterruptedException ignored) {}


			final Thread currentThread = Thread.currentThread();

			engine.execute(new Runnable() {
				@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
				@Override
				public void run() {
					SqlSession session = engine.openSession(shardKey, closure.getExecutorType());
					U res = null;
					try {
						res = closure.call(session);
						if (doCommit()) {
							session.commit();
						}
					} catch (Throwable t) {
						if (doCommit()) {
							session.rollback();
						}
						errors.add(new QueryError(shardKey, t, currentThread.getStackTrace()));
					} finally {
						session.close();
					}

					lock.lock();
					try {
						processResult(result, res);
						threadsCount.setValue(threadsCount.getValue() - 1);
						condition.signal();
					} finally {
						lock.unlock();
					}
				}
			});
		}

		lock.lock();
		try {
			while (! (this.<U>checkResultFinish(result) || threadsCount.getValue() == 0)) {
				condition.await();
			}
		} catch (InterruptedException e) {
			logger().error("Query was interrupted.", e);
		} finally {
			lock.unlock();
		}

		U resultValue = this.<U>extractResultValue(result);
		if (! errors.isEmpty()) {
			logErrors(errors, resultValue);
			throw new QueryException("Error of execution.", errors);
		}

		return resultValue;
	}

	protected <U> void logErrors(List<QueryError> errors, U resultValue) {
		logErrors(logger(), resultValue, errors);
	}

	protected boolean doCommit() {
		return false;
	}

	protected abstract <U> Object createResult();

	protected abstract <U> void processResult(Object result, U threadResult);

	protected abstract <U> boolean checkResultFinish(Object result);

	protected abstract <U> U extractResultValue(Object result);

	protected abstract Logger logger();
}
