package net.thumbtack.sharding.core.query;

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

/**
 * The base class for asynchronous queries.
 */
public abstract class QueryAsync implements Query {

    private ExecutorService executor;

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
//            // we can't do it too fast since db (or MyBatis) returns null by unclear reason
//            // so send thread to sleep 1 ms
//            try {
//                Thread.sleep(1);
//            } catch (InterruptedException ignored) {}


            final Thread currentThread = Thread.currentThread();

            executor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    if (logger().isDebugEnabled()) {
                        logger().debug(connection.toString());
                    }

                    connection.open();
                    U res = null;
                    try {
                        res = closure.call(connection);
                        if (isUpdate()) {
                            connection.commit();
                        }
                    } catch (Throwable t) {
                        if (isUpdate()) {
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
            // TODO this throw should be configurable. We need to have opportunity to get result even if some shards failed
            throw new QueryException("Error of execution.", errors);
        }

        return resultValue;
    }

    @Override
    public boolean isUpdate() {
        return false;
    }

    /**
     * Invokes when some errors occurred during query execution. Can be used to log some additional information.
     * @param errors The errors which occurred.
     * @param resultValue The result, null if there is no result.
     * @param <U> The result type.
     */
    protected <U> void logErrors(List<QueryError> errors, U resultValue) {
        logErrors(logger(), resultValue, errors);
    }

    /**
     * Creates some container for result of the query.
     * This container is common for all threads and for each thread will be invoked
     * {@link #processResult(Object, Object)} to process thread's result.
     * Also, this container should be tread-safe.
     * @param <U> The type of the result.
     * @return The container for query result.
     */
    @SuppressWarnings("UnusedDeclaration")
    protected abstract <U> Object createResult();

    /**
     * Processes the part of query result which returned from one thread.
     * @param result The container which contains full query result.
     * @param threadResult The result from one thread.
     * @param <U> The type of the result.
     */
    protected abstract <U> void processResult(Object result, U threadResult);

    /**
     * Checks should we finish with current result or not.
     * @param result The current result.
     * @param <U> The type of the result.
     * @return True if finish false otherwise.
     */
    @SuppressWarnings("UnusedDeclaration")
    protected abstract <U> boolean checkResultFinish(Object result);

    /**
     * Extract the result of query from container which was created with {@link #createResult()}.
     * @param result The container with result.
     * @param <U> The type of the result.
     * @return The result of the query.
     */
    protected abstract <U> U extractResultValue(Object result);

    /**
     * Gets logger from implementation to log.
     * @return The logger
     */
    protected abstract Logger logger();

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private static void logErrors(Logger logger, Object result, List<QueryError> errors) {
        StringBuilder sb = new StringBuilder();
        for (QueryError error : errors) {
            sb.append("On shard #").append(error.getShard()).append(":\n");
            if (error.getError() != null) {
                if (error.getError().getMessage() != null) {
                    sb.append(error.getError().getMessage()).append("\n");
                }
                sb.append(printStackTrace(error.getError().getStackTrace()));
            }
            sb.append("Parent stack:\n").append(printStackTrace(error.getParentStackTrace()));
        }
        logger.error("Query was finished with result {} end errors\n{}", result, sb.toString());
    }

    private static String printStackTrace(StackTraceElement[] stackTrace) {
        if (stackTrace != null) {
            StringBuilder sb = new StringBuilder();
            for (StackTraceElement trace : stackTrace) {
                sb.append("\tat ").append(trace).append("\n");
            }

            return sb.toString();
        }
        return "";
    }

    /**
     * Sets the threadPool.
     * @param executor The thread pool.
     */
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }
}
