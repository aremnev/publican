package net.thumbtack.sharding.vbucket;

import net.thumbtack.sharding.core.KeyMapper;
import net.thumbtack.sharding.core.Shard;

import java.util.Map;

/**
 *
 */
public class VbucketEngine {

    private VbucketMapper mapper;

    public VbucketEngine(Map<Integer, Integer> bucketToShard) {
        final long bucketSize = Long.MAX_VALUE / bucketToShard.size() + 1;
        mapper = new VbucketMapper(bucketToShard, new KeyMapper() {
            @Override
            public int shard(long key) {
                return (int) (key / bucketSize + 1);
            }
        });
    }

    public KeyMapper getMapper() {
        return mapper;
    }

    public Shard getShard(int shardId) {
        return null;
    }
}
