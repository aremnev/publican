package net.thumbtack.sharding;

import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.sql.SQLException;

public class MemcachedServer {

    private static final Logger logger = LoggerFactory.getLogger(MemcachedServer.class);

    final MemCacheDaemon<LocalCacheElement> daemon = new MemCacheDaemon<LocalCacheElement>();

    public MemcachedServer(int port) {
        int maxItems = 10000;
        long maxBytes = 10000000;
        InetSocketAddress addr = new InetSocketAddress(port);
        // create daemon and start it
        CacheStorage<Key, LocalCacheElement> storage = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.FIFO, maxItems, maxBytes);
        daemon.setCache(new CacheImpl(storage));
//        daemon.setBinary(binary);
        daemon.setAddr(addr);
//        daemon.setIdleTime(idle);
//        daemon.setVerbose(verbose);
//        if (server != null)
//            throw new RuntimeException("H2 server already started");
//        // start H2 server here
//        org.h2.Driver.load();
//        String[] params = new String[] {"-tcp", "-tcpAllowOthers"};
//        server = Server.createTcpServer(params);
//        server.start();
//        logger.debug("Database server started");
//        return server;
    }

    public void start() throws SQLException {
        daemon.start();
    }

    public void stop() {
        daemon.stop();
        logger.debug("Database server stopped");
    }
}
