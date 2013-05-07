package net.thumbtack.sharding.vbucket;

import net.thumbtack.sharding.vbucket.VbucketState;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class VbucketMapperBuilder {

    private int bucketsCount;

    /**
     * Maps the bucket to the shard where this bucket is active.
     */
    private Map<Integer, Integer> bucket2shardMap;

    public VbucketMapperBuilder(Map<Integer, List<VbucketState[]>> shardsState) {

        this.bucketsCount = bucketsCount;
        this.bucket2shardMap = bucket2shardMap;
    }

    public int getBucketsCount() {
        return bucketsCount;
    }

    public void setBucketsCount(int bucketsCount) {
        this.bucketsCount = bucketsCount;
    }
}
