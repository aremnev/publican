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

public class VbucketMigration implements Callable<Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(VbucketMigration.class);

    private final Vbucket bucket;
    private final Shard toShard;
    private final Shard fromShard;
    private final VbucketMigrationHelper helper;

    public VbucketMigration(Vbucket bucket, Shard fromShard, Shard toShard, VbucketMigrationHelper helper) {
        this.bucket = bucket;
        this.fromShard = fromShard;
        this.toShard = toShard;
        this.helper = helper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Boolean call() throws Exception {
        logger.debug("Migrating bucket {} from shard {} to shard {}", bucket.id, fromShard.getId(), toShard.getId());

        final List<Connection> connFrom = getShardConnection(fromShard);
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
                        update.query(helper.putEntity(entity), connTo);
                    }

                    return f(lastModificationTime);
                } catch (Exception e) {
                    logger.error("Migration failed with error", e);
                    return false;
                }
            }
        };

        return migrationStep.f(0L);
    }

    private List<Connection> getShardConnection(Shard shard) {
        List<Connection> connections = new ArrayList<Connection>(1);
        connections.add(shard.getConnection());
        return connections;
    }
}
