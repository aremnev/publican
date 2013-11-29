package net.thumbtack.shardcon.core;

import org.apache.commons.lang3.mutable.Mutable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class QueryLock {

    private Set<Long> queryIds;
    private long timeout;
    private Lock lock;
    private Mutable<Boolean> isLocked;

    public QueryLock(Lock lock, Mutable<Boolean> isLocked, List<Long> queryIds) {
        this(lock, Long.MAX_VALUE, TimeUnit.MILLISECONDS, isLocked, queryIds);
    }

    public QueryLock(Lock lock, long timeout, TimeUnit timeUnit, Mutable<Boolean> isLocked, List<Long> queryIds) {
        this.lock = lock;
        this.isLocked = isLocked;
        this.timeout = timeUnit.toMillis(timeout);
        this.queryIds = new HashSet<>(queryIds);
    }

    public void lock() {
        if (! queryIds.isEmpty()) {
            lock.lock();
            isLocked.setValue(true);
        }
    }

    public void unlock() {
        isLocked.setValue(false);
        lock.unlock();
    }

    public void await(long queryId) {
        if (isLocked.getValue() && ! queryIds.isEmpty() && queryIds.contains(queryId)) {
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
