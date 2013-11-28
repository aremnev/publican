package net.thumbtack.sharding.vbucket;

import java.util.ArrayList;
import java.util.List;

public class Vbucket {

    public final int id;
    public final long fromId;
    public final long toId;

    public Vbucket(int id, long fromId, long toId) {
        this.id = id;
        this.fromId = fromId;
        this.toId = toId;
    }

    public static List<Vbucket> buildBuckets(int count) {
        List<Integer> bucketIds = new ArrayList<Integer>(count);
        for (int i = 0; i < count; i++) {
            bucketIds.add(i);
        }
        List<Vbucket> buckets = new ArrayList<Vbucket>(count);
        long bucketSize = bucketSize(count);
        for (int id : bucketIds) {
            long fromId = id * bucketSize;
            long toId = fromId + (bucketSize - 1);
            buckets.add(new Vbucket(id, fromId, toId));
        }
        return buckets;
    }

    public static long bucketSize(int count) {
        return Long.MAX_VALUE / count + 1;
    }

    @Override
    public String toString() {
        return "Vbucket{" +
                "id=" + id +
                ", fromId=" + fromId +
                ", toId=" + toId +
                '}';
    }
}
