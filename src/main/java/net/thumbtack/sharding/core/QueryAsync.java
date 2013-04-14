package net.thumbtack.sharding.core;

import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class QueryAsync extends Query {

    private Executor executor;

    public QueryAsync(Executor executor) {
        this.executor = executor;
    }

    @Override
    public <U> U query(final QueryClosure<U> closure, List<Connection> shards) {
        final Lock lock = new ReentrantLock();
        final Condition condition = lock.newCondition();
        // counter of threads that already executed
        final MutableInt threadsCount = new MutableInt(shards.size());
        final Object result = this.<U>createResult();
        // all errors are accumulated here
        final List<QueryError> errors = Collections.synchronizedList(new ArrayList<QueryError>());

        // for each shard one thread is run
        // threads are synchronized by lock
        // when thread has done it send condition.signal();
        for (final Connection connection : shards) {
            // we can't do it too fast since db (or MyBatis) returns null by unclear reason
            // so send thread to sleep 1 ms
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignored) {}


            final Thread currentThread = Thread.currentThread();

            executor.execute(new Runnable() {
                @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
                @Override
                public void run() {
                    connection.open();
                    U res = null;
                    try {
                        res = closure.call(connection);
                        if (doCommit()) {
                            connection.commit();
                        }
                    } catch (Throwable t) {
                        if (doCommit()) {
                            connection.rollback();
                        }
                        errors.add(new QueryError(connection.toString(), t, currentThread.getStackTrace()));
                    } finally {
                        connection.close();
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
