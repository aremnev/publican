package net.thumbtack.sharding.core;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

public class QueryLock {

    private Set<Long> queryIds;
    private long timeout;
    private Lock lock;
    private Value<Boolean> isLocked;

    public QueryLock(Lock lock, Value<Boolean> isLocked, long... queryIds) {
        this(lock, Long.MAX_VALUE, TimeUnit.MILLISECONDS, isLocked, queryIds);
    }

    public QueryLock(Lock lock, long timeout, TimeUnit timeUnit, Value<Boolean> isLocked, long... queryIds) {
        this.lock = lock;
        this.isLocked = isLocked;
        this.timeout = timeUnit.toMillis(timeout);
        this.queryIds = new HashSet<Long>(queryIds.length);
        for (long queryId : queryIds) {
            this.queryIds.add(queryId);
        }
    }

    public void lock(long queryId) {
        if (! queryIds.isEmpty() && queryIds.contains(queryId)) {
            lock.lock();
            isLocked.set(true);
        }
    }

    public void unlock() {
        isLocked.set(false);
        lock.unlock();
    }

    public void await(long queryId) {
        if (isLocked.get() && ! queryIds.isEmpty() && queryIds.contains(queryId)) {
            try {
                if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
