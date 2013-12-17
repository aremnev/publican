package net.thumbtack.shardcon.chunk;

import fj.F;
import net.thumbtack.shardcon.core.Shard;
import net.thumbtack.shardcon.core.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class Migration implements Callable<Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(Migration.class);

    private final Chunk bucket;
    private final Shard toShard;
    private final Shard fromShard;
    private final MigrationHelper helper;

    public Migration(Chunk bucket, Shard fromShard, Shard toShard, MigrationHelper helper) {
        this.bucket = bucket;
        this.fromShard = fromShard;
        this.toShard = toShard;
        this.helper = helper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Boolean call() throws Exception {
        logger.info("Migrating bucket {} from shard {} to shard {}", bucket.id, fromShard.getId(), toShard.getId());

        final Query select = new SelectSpecShard();
        final Query update = new UpdateSpecShard();
        final QueryClosure<Long> lastModificationF = helper.getLastModificationTime(bucket.fromId, bucket.toId);

        // moves all the entities recursively
        F<Long, Boolean> migrationStep = new F<Long, Boolean>() {
            @Override
            public Boolean f(Long timeFrom) {
                try {
                    long lastModificationTime = select.query(lastModificationF, getShardConnection(fromShard));
                    QueryClosure<List<Long>> modifiedAfterF = helper.getModifiedAfter(bucket.fromId, bucket.toId, timeFrom);
                    List<Long> ids = select.query(modifiedAfterF, getShardConnection(fromShard));
                    if (ids == null || ids.isEmpty()) {
                        return true;
                    }

                    for (long id : ids) {
                        Object entity = select.query(helper.getEntity(id), getShardConnection(fromShard));
                        update.query(helper.putEntity(entity), getShardConnection(toShard));
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
