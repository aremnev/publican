package net.thumbtack.sharding;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory with distinguishable names
 */
public class NamedThreadFactory implements ThreadFactory {

	static final AtomicInteger poolNumber = new AtomicInteger(1);

	final ThreadGroup group;
	final AtomicInteger threadNumber = new AtomicInteger(1);
	final String namePrefix;

	public NamedThreadFactory(String name) {
		SecurityManager s = System.getSecurityManager();

		group = (s != null)? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
		namePrefix = "pool-" + poolNumber.getAndIncrement() + "(" + name + ")-thread-";
	}

	public Thread newThread(Runnable r) {
		Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);

		if (t.isDaemon()) {
			t.setDaemon(false);
		}

		if (t.getPriority() != Thread.NORM_PRIORITY){
			t.setPriority(Thread.NORM_PRIORITY);
		}

		return t;
	}
}
