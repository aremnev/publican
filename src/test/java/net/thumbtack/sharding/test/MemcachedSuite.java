package net.thumbtack.sharding.test;

import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import net.thumbtack.sharding.H2Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;


public class MemcachedSuite {

    private static final Logger logger = LoggerFactory.getLogger(MemcachedSuite.class);

    H2Server h2Server = new H2Server();

    final MemCacheDaemon<LocalCacheElement> daemon1 = new MemCacheDaemon<LocalCacheElement>();
    final MemCacheDaemon<LocalCacheElement> daemon2 = new MemCacheDaemon<LocalCacheElement>();

    public MemcachedSuite() throws Exception {
        int maxItems = 10000;
        long maxBytes = 10000000;
        int port1 = 11213;
        int port2 = 11212;
        InetSocketAddress addr1 = new InetSocketAddress(port1);
        InetSocketAddress addr2 = new InetSocketAddress(port2);
        // create daemon and start it
        CacheStorage<Key, LocalCacheElement> storage1 = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.FIFO, maxItems, maxBytes);
        daemon1.setCache(new CacheImpl(storage1));
//        daemon.setBinary(binary);
        daemon1.setAddr(addr1);
//        daemon.setIdleTime(idle);
//        daemon.setVerbose(verbose);
        CacheStorage<Key, LocalCacheElement> storage2 = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.FIFO, maxItems, maxBytes);
        daemon2.setCache(new CacheImpl(storage2));
        daemon2.setAddr(addr2);

    }

    public void start() throws Exception {
        daemon1.start();
        daemon2.start();
        logger.debug("embedded memcached server started");
    }

    public void stop() throws Exception {
        daemon1.stop();
        daemon2.stop();
        logger.debug("embedded memcached server stopped");
    }
}
