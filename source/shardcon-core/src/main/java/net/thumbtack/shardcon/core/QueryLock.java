package net.thumbtack.shardcon.core;

import org.apache.commons.lang3.mutable.Mutable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

public class QueryLock {

    private Set<Long> queriesToLock;
    private Lock lock;
    private Mutable<Boolean> isLocked;

    public QueryLock(Lock lock, Mutable<Boolean> isLocked, List<Long> queriesToLock) {
        this.lock = lock;
        this.isLocked = isLocked;
        this.queriesToLock = new HashSet<>(queriesToLock);
    }

    public void lock() {
        if (! queriesToLock.isEmpty()) {
            lock.lock();
            isLocked.setValue(true);
        }
    }

    public void unlock() {
        isLocked.setValue(false);
        lock.unlock();
    }

    public void await(long queryId) {
        if (isLocked.getValue() && ! queriesToLock.isEmpty() && queriesToLock.contains(queryId)) {
            if (lock.tryLock()) {
                lock.unlock();
            }
        }
    }
}
