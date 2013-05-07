package net.thumbtack.sharding.vbucket;

import fj.F;
import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VbucketMigration {

    private static final Logger logger = LoggerFactory.getLogger(VbucketMigration.class);

    private VbucketMigrationHelper helper;

    private ExecutorService executor;

    public VbucketMigration(VbucketMigrationHelper migrationHelper) {
        this.helper = migrationHelper;
        executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("chunk-migration"));
    }

    public void migrate(final Vbucket bucket, final int toShard) {
        executor.submit(new Callable<Void>() {
            @SuppressWarnings("unchecked")
            @Override
            public Void call() throws Exception {

                final Connection connFrom = getBucketConnection(bucket.id);
                final Connection connTo = getShardConnection(toShard);
                final QueryClosure<Long> lastModificationF = helper.getLastModificationTime(bucket.idFrom, bucket.idTo);

                F<Long, Void> migrationStep = new F<Long, Void>() {
                    @Override
                    public Void f(Long timeFrom) {
                        try {
                            long lastModificationTime = lastModificationF.call(connFrom);
                            QueryClosure<List<Long>> modifiedAfterF = helper.getModifiedAfter(timeFrom);
                            List<Long> ids = modifiedAfterF.call(connFrom);
                            if (ids == null || ids.isEmpty()) {
                                return null;
                            }

                            for (long id : ids) {
                                Object entity = helper.getEntity(id).call(connFrom);
                                helper.setEntity(entity).call(connTo);
                            }

                            return f(lastModificationTime);
                        } catch (Exception e) {
                            logger.error("Migration failed with error", e);
                            return null;
                        }
                    }
                };

                return migrationStep.f(0L);
            }
        });
    }

    private Connection getShardConnection(int shradId) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    private Connection getBucketConnection(int bucketId) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }
}
