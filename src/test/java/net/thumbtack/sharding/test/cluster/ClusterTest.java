package net.thumbtack.sharding.test.cluster;

import net.thumbtack.helper.Util;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.jdbc.JdbcConnection;
import net.thumbtack.sharding.test.common.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static net.thumbtack.sharding.TestUtil.generateUID;

public class ClusterTest {

    private static final Logger logger = LoggerFactory.getLogger(ClusterTest.class);

    private ShardedDao dao;

    private List<Entity> entities;

    public static void main(String[] args) throws Exception {
        ClusterInstance clusterInstance = new ClusterInstance();
        clusterInstance.start();
        clusterInstance.getSharding().updateAll(new QueryClosure<Object>() {
            @Override
            public Object call(net.thumbtack.sharding.core.query.Connection connection) throws Exception {
                Connection sqlConn = ((JdbcConnection) connection).getConnection();
                sqlConn.createStatement().execute("DROP TABLE IF EXISTS `sharded`;");
                return null;
            }
        });
        clusterInstance.getSharding().updateAll(new QueryClosure<Object>() {
            @Override
            public Object call(net.thumbtack.sharding.core.query.Connection connection) throws Exception {
                Connection sqlConn = ((JdbcConnection) connection).getConnection();
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
        ClusterTest test = new ClusterTest(dao);
        test.init();
        Process anotherInstance = startAnotherInstance();
    }

    private static Process startAnotherInstance() throws Exception {
        String separator = System.getProperty("file.separator");
        String classpath = System.getProperty("java.class.path");
        String path = System.getProperty("java.home") + separator + "bin" + separator + "java";
        ProcessBuilder processBuilder = new ProcessBuilder(path, "-cp", classpath, ClusterInstance.class.getCanonicalName());
        final Process process = processBuilder.start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String line;
                    while ((line = br.readLine()) != null) {
                        logger.info("Another instance " + line);
                    }
                } catch (Exception ignored) {}
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                    String line;
                    while ((line = br.readLine()) != null) {
                        logger.info("Another instance " + line);
                    }
                } catch (Exception ignored) {}
            }
        }).start();
        return process;
    }

    public ClusterTest(ShardedDao dao) {
        this.dao = dao;
    }

    public void init() {
        dao.deleteAll();
        logger.info("initialising...");
        RandomString randomString = new RandomString(new Random(), 16);
        entities = new ArrayList<Entity>(1000);
        for (int i = 0; i < 1000; i++) {
            entities.add(new Entity(generateUID(), randomString.nextString(), new Date()));
        }
        dao.insert(entities);
        logger.info("initialising finished");
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
