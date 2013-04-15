package net.thumbtack.sharding.core;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class QueryAsync extends Query {

    private ExecutorService executor;

    public QueryAsync(ExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public <U> U query(final QueryClosure<U> closure, List<Connection> shards) {
        final Lock lock = new ReentrantLock();
        final Condition condition = lock.newCondition();
        // counter of threads that already executed
        final AtomicInteger threadsCount = new AtomicInteger(shards.size());
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

            executor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    logger().debug(connection.toString());
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
                        threadsCount.set(threadsCount.get() - 1);
                        condition.signal();
                    } finally {
                        lock.unlock();
                    }
                    return null;
                }
            });
        }

        lock.lock();
        try {
            while (! (this.<U>checkResultFinish(result) || threadsCount.get() == 0)) {
                condition.await();
            }
        } catch (InterruptedException e) {
            logger().error("Query was interrupted.", e);
        } finally {
            lock.unlock();
        }

        U resultValue = this.extractResultValue(result);
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

    @SuppressWarnings("UnusedDeclaration")
    protected abstract <U> Object createResult();

    protected abstract <U> void processResult(Object result, U threadResult);

    @SuppressWarnings("UnusedDeclaration")
    protected abstract <U> boolean checkResultFinish(Object result);

    protected abstract <U> U extractResultValue(Object result);

    protected abstract Logger logger();
}
