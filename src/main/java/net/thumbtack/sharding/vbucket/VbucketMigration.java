package net.thumbtack.sharding.vbucket;

import fj.F;
import net.thumbtack.sharding.core.KeyMapper;
import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class VbucketMigration implements Callable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(VbucketMigration.class);
    private final Vbucket bucket;
    private final int toShard;
    private final VbucketEngine engine;
    private final VbucketMigrationHelper helper;

    public VbucketMigration(Vbucket bucket, int toShard, VbucketEngine engine, VbucketMigrationHelper helper) {
        this.bucket = bucket;
        this.toShard = toShard;
        this.engine = engine;
        this.helper = helper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Void call() throws Exception {
        final List<Connection> connFrom = getBucketConnection(bucket);
        final List<Connection> connTo = getShardConnection(toShard);
        final Query select = new SelectSpecShard();
        final Query update = new UpdateSpecShard();
        final QueryClosure<Long> lastModificationF = helper.getLastModificationTime(bucket.fromId, bucket.toId);

        // moves all the entities recursively
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

        if (migrationStep.f(0L)) {
            helper.onFinish(bucket.id, toShard);
            helper.remove(bucket.fromId, bucket.toId);
        }
        return null;
    }

    private List<Connection> getShardConnection(int shardId) {
        Shard shard = engine.getShard(shardId);
        List<Connection> connections = new ArrayList<Connection>(1);
        connections.add(shard.getConnection());
        return connections;
    }

    private List<Connection> getBucketConnection(Vbucket bucket) {
        KeyMapper mapper = engine.getMapper();
        int shardId = mapper.shard(bucket.fromId);
        return getShardConnection(shardId);
    }
}
