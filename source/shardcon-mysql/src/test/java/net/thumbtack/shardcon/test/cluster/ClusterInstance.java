package net.thumbtack.shardcon.test.cluster;

import fj.F;
import net.thumbtack.helper.Util;
import net.thumbtack.shardcon.ShardingFacade;
import net.thumbtack.shardcon.chunk.ChunkEngine;
import net.thumbtack.shardcon.chunk.MigrationHelper;
import net.thumbtack.shardcon.chunk.MigrationInfo;
import net.thumbtack.shardcon.chunk.MovedEvent;
import net.thumbtack.shardcon.core.Shard;
import net.thumbtack.shardcon.core.ShardingBuilder;
import net.thumbtack.shardcon.core.cluster.*;
import net.thumbtack.shardcon.core.query.Query;
import net.thumbtack.shardcon.impl.jdbc.JdbcShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static net.thumbtack.shardcon.ShardingFacade.*;
import static net.thumbtack.helper.Util.*;

public class ClusterInstance {

    private static final Logger logger = LoggerFactory.getLogger(ClusterInstance.class);

    private ShardingFacade sharding;
    private ShardingCluster cluster;
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
        Map<Long, Query> queryMap = getQueryMap(false);
        for (long queryId : queryMap.keySet()) {
            builder.addQuery(queryId, queryMap.get(queryId));
        }

        cluster = new HazelcastShardingCluster()
                .addHost("localhost")
                .addQueryToLock(SELECT_ALL_SHARDS)
                .addQueryToLock(SELECT_ALL_SHARDS_SUM)
                .addQueryToLock(UPDATE_ALL_SHARDS)
                .addQueryToLock(UPDATE_SPEC_SHARD)
                .start();
        chunkEngine.setShardingCluster(cluster);
        builder.setShardingCluster(cluster);

        cluster.addEventProcessor(new EventProcessor() {
            @Override
            public void onEvent(Event event) {
                if (event.getId() == MovedEvent.ID) {
                    final MigrationInfo migrationInfo = ((MovedEvent) event).getEventObject();
                    Shard shard = find(shards, new F<Shard, Boolean>() {
                        @Override
                        public Boolean f(Shard shard) {
                            return shard.getId() == migrationInfo.getToShardId();
                        }
                    });
                    bucketToShard.put(migrationInfo.getBucketId(), shard);
                    completed = true;
                }
            }

            @Override
            public void setEventListener(EventListener listener) {}
        });

        sharding = new ShardingFacade(builder.build());
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
