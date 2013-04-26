package net.thumbtack.sharding.core.map;

import java.util.List;
import java.util.Map;

public class VbucketConfig {

    private int bucketsCount;

    /**
     * Maps the bucket to the shard where this bucket is active.
     */
    private Map<Integer, Integer> bucket2shardMap;

    public VbucketConfig(List<VbucketState[]> shardsState) {

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
