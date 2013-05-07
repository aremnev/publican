package net.thumbtack.sharding.vbucket;

import fj.F;
import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VbucketMigration {

    private static final Logger logger = LoggerFactory.getLogger(VbucketMigration.class);

    private final VbucketMapper mapper;
    private final Map<Integer, Shard> shards;
    private VbucketMigrationHelper helper;
    private MigrationFinishedListener finishedListener;

    private ExecutorService executor;

    public VbucketMigration(VbucketMapper mapper, Map<Integer, Shard> shards, VbucketMigrationHelper migrationHelper, MigrationFinishedListener finishedListener) {
        this.mapper = mapper;
        this.shards = shards;
        this.helper = migrationHelper;
        this.finishedListener = finishedListener;
        executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("chunk-migration"));
    }

    public void migrate(final Vbucket bucket, final int toShard) {
        executor.submit(new Callable<Void>() {
            @SuppressWarnings("unchecked")
            @Override
            public Void call() throws Exception {

                final List<Connection> connFrom = getBucketConnection(bucket);
                final List<Connection> connTo = getShardConnection(toShard);
                final Query select = new SelectSpecShard();
                final Query update = new UpdateSpecShard();
                final QueryClosure<Long> lastModificationF = helper.getLastModificationTime(bucket.idFrom, bucket.idTo);

                F<Long, Boolean> migrationStep = new F<Long, Boolean>() {
                    @Override
                    public Boolean f(Long timeFrom) {
                        try {
                            long lastModificationTime = select.query(lastModificationF, connFrom);
                            QueryClosure<List<Long>> modifiedAfterF = helper.getModifiedAfter(timeFrom);
                            List<Long> ids = select.query(modifiedAfterF, connFrom);
                            if (ids == null || ids.isEmpty()) {
                                return true;
                            }

                            for (long id : ids) {
                                Object entity = select.query(helper.getEntity(id), connFrom);
                                update.query(helper.setEntity(entity), connTo);
                            }

                            return f(lastModificationTime);
                        } catch (Exception e) {
                            logger.error("Migration failed with error", e);
                            return false;
                        }
                    }
                };

                boolean finished = migrationStep.f(0L);
                if (finished && finishedListener != null) {
                    finishedListener.onFinish(bucket.id, toShard);
                }
                return null;
            }
        });
    }

    private List<Connection> getShardConnection(int shardId) {
        Shard shard = shards.get(shardId);
        List<Connection> connections = new ArrayList<Connection>(1);
        connections.add(shard.getConnection());
        return connections;
    }

    private List<Connection> getBucketConnection(Vbucket bucket) {
        int shardId = mapper.shard(bucket.idFrom);
        return getShardConnection(shardId);
    }

    public interface MigrationFinishedListener {
        void onFinish(int bucketId, int toShard);
    }
}
