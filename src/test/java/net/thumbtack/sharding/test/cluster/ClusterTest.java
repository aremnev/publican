package net.thumbtack.sharding.test.cluster;

import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.jdbc.JdbcConnection;
import net.thumbtack.sharding.test.common.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.Assert.assertEquals;
import static net.thumbtack.sharding.TestUtil.generateUID;

public class ClusterTest {

    private static final Logger logger = LoggerFactory.getLogger(ClusterTest.class);

    private static ClusterInstance clusterInstance;

    private ShardedDao dao;

    private List<Entity> entities;

    public static void main(String[] args) throws Exception {
        clusterInstance = new ClusterInstance();
        clusterInstance.start();
        clusterInstance.getSharding().updateAll(new QueryClosure<Object>() {
            @Override
            public Object call(net.thumbtack.sharding.core.query.Connection connection) throws Exception {
                Connection sqlConn = ((JdbcConnection) connection).getConnection();
                sqlConn.createStatement().execute("DROP TABLE IF EXISTS `sharded`;");
                sqlConn.createStatement().execute("CREATE TABLE `sharded` (\n" +
                        "  `id` bigint(20) unsigned NOT NULL,\n" +
                        "  `text` varchar(255) COLLATE utf8_bin NOT NULL,\n" +
                        "  `date` TIMESTAMP NOT NULL DEFAULT '2011-12-31 23:59:59',\n" +
                        "  `timestamp` bigint(20) unsigned NOT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;");
                return null;
            }
        });
        ShardedDao dao = new ShardedDao(clusterInstance.getSharding());
        final ClusterTest test = new ClusterTest(dao);
        test.init();
        Process anotherInstance = startAnotherInstance();
        Thread.sleep(10000);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<Void> futureSelect = executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                test.testSelects(); return null;
            }
        });
        futureSelect.get();

        Future<Void> futureDelete = executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                test.testDeletes(); return null;
            }
        });
        futureDelete.get();

        anotherInstance.destroy();
        clusterInstance.shutdown();
    }

    private void testDeletes() {
        Random random = new Random();
        int steps = 1000;
        for (int i = 0; i < steps; i++) {
            Entity e = entities.get(random.nextInt(entities.size()));
            dao.delete(e);
            Entity selected = clusterInstance.getSharding().select(dao.getSelectClosure(e.id));
            assertEquals(null, selected);
            dao.insert(e);

            double p = ((double) i / steps) * 100;
            if (Math.floor(p) == p) {
                logger.info("testDeletes " + p + "%");
            }
        }
    }

    private void testSelects() {
        Random random = new Random();
        int steps = 10000;
        for (int i = 0; i < steps; i++) {
            Entity e = entities.get(random.nextInt(entities.size()));
            Entity selected = dao.select(e.id);
            assertEquals(e, selected);

            double p = ((double) i / steps) * 100;
            if (Math.floor(p) == p) {
                logger.info("testSelects " + p + "%");
            }
        }
    }

    private static Process startAnotherInstance() throws Exception {
        String separator = System.getProperty("file.separator");
        String classpath = System.getProperty("java.class.path");
        String path = System.getProperty("java.home") + separator + "bin" + separator + "java";
        ProcessBuilder processBuilder = new ProcessBuilder(path, "-cp", classpath, ClusterInstance.class.getCanonicalName());
        final Process process = processBuilder.start();
        // read process output
        new Thread(new OutStream(process.getInputStream())).start();
        new Thread(new OutStream(process.getErrorStream())).start();
        return process;
    }

    public ClusterTest(ShardedDao dao) {
        this.dao = dao;
    }

    public void init() {
        dao.deleteAll();
        logger.info("initialising...");
        RandomString randomString = new RandomString(new Random(), 16);
        int entitiesCount = 1000;
        entities = Collections.synchronizedList(new ArrayList<Entity>(entitiesCount));
        for (int i = 0; i < entitiesCount; i++) {
            entities.add(new Entity(generateUID(), randomString.nextString(), new Date()));
        }
        dao.insert(entities);
        entities.clear();
        entities.addAll(dao.selectAll());
        assertEquals(entitiesCount, entities.size());
        logger.info("initialising finished");
    }

    private static class OutStream implements Runnable {

        private InputStream inputStream;

        public OutStream(InputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public void run() {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                while ((line = br.readLine()) != null) {
                    logger.info("Another instance " + line);
                }
            } catch (Exception e) {
                logger.error("Error during reading stream", e);
            }
        }
    }

    private static class RandomString {
        private static final char[] symbols = new char[36];
        private final char[] buf;
        private final Random random;

        static {
            for (int idx = 0; idx < 10; ++idx)
                symbols[idx] = (char) ('0' + idx);
            for (int idx = 10; idx < 36; ++idx)
                symbols[idx] = (char) ('a' + idx - 10);
        }

        public RandomString(Random random, int length) {
            this.random = random;
            if (length < 1)
                throw new IllegalArgumentException("length < 1: " + length);
            buf = new char[length];
        }

        public String nextString() {
            for (int idx = 0; idx < buf.length; ++idx)
                buf[idx] = symbols[random.nextInt(symbols.length)];
            return new String(buf);
        }

    }
}
