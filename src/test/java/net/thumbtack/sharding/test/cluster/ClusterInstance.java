package net.thumbtack.sharding.test.cluster;

import fj.F;
import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.ShardingBuilder;
import net.thumbtack.sharding.core.cluster.*;
import net.thumbtack.sharding.core.query.Query;
import net.thumbtack.sharding.impl.jdbc.JdbcShard;
import net.thumbtack.sharding.vbucket.VbucketEngine;
import net.thumbtack.sharding.vbucket.VbucketMigrationHelper;
import net.thumbtack.sharding.vbucket.VbucketMigrationInfo;
import net.thumbtack.sharding.vbucket.VbucketMovedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static net.thumbtack.sharding.ShardingFacade.*;
import static net.thumbtack.helper.Util.*;

public class ClusterInstance {

    private static final Logger logger = LoggerFactory.getLogger(ClusterInstance.class);

    private ShardingFacade sharding;
    private ShardingCluster cluster;
    private VbucketEngine vbucketEngine;

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
        bucketToShard = VbucketEngine.mapBucketsFromProperties(shards, shardProps);
        vbucketEngine = new VbucketEngine(bucketToShard);
        ShardingBuilder builder = new ShardingBuilder();
        builder.setShards(shards);
        builder.setKeyMapper(vbucketEngine);
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
        vbucketEngine.setShardingCluster(cluster);
        builder.setShardingCluster(cluster);

        cluster.addEventProcessor(new EventProcessor() {
            @Override
            public void onEvent(Event event) {
                if (event.getId() == VbucketMovedEvent.ID) {
                    final VbucketMigrationInfo migrationInfo = ((VbucketMovedEvent) event).getEventObject();
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
        VbucketMigrationHelper migrationHelper = new VbucketMigrationHelperImpl();
        while (true) {
            int randomBucket = random.nextInt(bucketToShard.size());
            Shard bucketShard = bucketToShard.get(randomBucket);
            int toShard = bucketShard.getId();
            while (toShard == bucketShard.getId()) {
                toShard = random.nextInt(shards.size());
            }
            completed = false;
            vbucketEngine.migrate(randomBucket, toShard, migrationHelper);
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

    public VbucketEngine getVbucketEngine() {
        return vbucketEngine;
    }
}
