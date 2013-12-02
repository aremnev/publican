package net.thumbtack.shardcon.cluster;

import fj.F;
import net.thumbtack.helper.Util;
import net.thumbtack.shardcon.chunk.ChunkEngine;
import net.thumbtack.shardcon.chunk.MigrationEvent;
import net.thumbtack.shardcon.chunk.MigrationHelper;
import net.thumbtack.shardcon.core.QueryLock;
import net.thumbtack.shardcon.core.Shard;
import net.thumbtack.shardcon.core.Sharding;
import net.thumbtack.shardcon.core.ShardingBuilder;
import net.thumbtack.shardcon.core.query.Query;
import net.thumbtack.shardcon.impl.jdbc.JdbcShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

import static net.thumbtack.helper.Util.*;

public class ClusterInstance {

    private static final Logger logger = LoggerFactory.getLogger(ClusterInstance.class);

    private ShardingFacade sharding;
    private HazelcastCluster cluster;
    private ChunkEngine chunkEngine;

    private volatile boolean completed = false;
    private List<Shard> shards;
    private Map<Integer, Shard> bucketToShard;

    public static void main(String[] args) throws Exception {
        ClusterInstance instance = new ClusterInstance();
        instance.start();
        instance.startMigrations();
    }

    public void start() throws Exception {
        logger.info("starting...");
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("MySql-shard.properties"));
        shards = JdbcShard.fromProperties(shardProps);
        bucketToShard = ChunkEngine.mapBucketsFromProperties(shards, shardProps);
        chunkEngine = new ChunkEngine(bucketToShard);
        ShardingBuilder builder = new ShardingBuilder();
        builder.setShards(shards);
        builder.setKeyMapper(chunkEngine);
        Map<Long, Query> queryMap = ShardingFacade.getQueryMap(false);
        for (long queryId : queryMap.keySet()) {
            builder.addQuery(queryId, queryMap.get(queryId));
        }

        List<Long> queriesToLock = new ArrayList<>();
        queriesToLock.add(ShardingFacade.SELECT_ALL_SHARDS);
        queriesToLock.add(ShardingFacade.SELECT_ALL_SHARDS_SUM);
        queriesToLock.add(ShardingFacade.UPDATE_ALL_SHARDS);
        queriesToLock.add(ShardingFacade.UPDATE_SPEC_SHARD);
        builder.setQueriesToLock(queriesToLock);

        cluster = new HazelcastCluster()
                .addHost("localhost")
                .start();

        builder.setShardingCluster(cluster);
        sharding = new ShardingFacade(builder.build());

        chunkEngine.setShardingCluster(cluster, queriesToLock);

        cluster.addMessageListener(new MessageListener() {
            @Override
            public void onMessage(Serializable event) {
                if (event instanceof MigrationEvent) {
                    final MigrationEvent migrationEvent = (MigrationEvent) event;
                    Shard shard = find(shards, new F<Shard, Boolean>() {
                        @Override
                        public Boolean f(Shard shard) {
                            return shard.getId() == migrationEvent.getToShardId();
                        }
                    });
                    bucketToShard.put(migrationEvent.getChunkId(), shard);
                    completed = true;
                }
            }
        });
    }

    public void shutdown() {
        cluster.shutdown();
    }

    public boolean startMigrations() {
        logger.info("starting migrations...");
        Random random = new Random();
        MigrationHelper migrationHelper = new MigrationHelperImpl();
        while (true) {
            int randomBucket = random.nextInt(bucketToShard.size());
            Shard bucketShard = bucketToShard.get(randomBucket);
            int toShard = bucketShard.getId();
            while (toShard == bucketShard.getId()) {
                toShard = random.nextInt(shards.size());
            }
            completed = false;
            chunkEngine.migrate(randomBucket, toShard, migrationHelper);
            while (! completed) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {}
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {}
        }
    }

    public ShardingFacade getSharding() {
        return sharding;
    }

    public ShardingCluster getCluster() {
        return cluster;
    }

    public ChunkEngine getChunkEngine() {
        return chunkEngine;
    }
}
