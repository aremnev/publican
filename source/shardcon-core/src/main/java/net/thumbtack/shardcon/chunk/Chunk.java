package net.thumbtack.shardcon.chunk;

import java.util.ArrayList;
import java.util.List;

public class Chunk {

    public final int id;
    public final long fromId;
    public final long toId;

    public Chunk(int id, long fromId, long toId) {
        this.id = id;
        this.fromId = fromId;
        this.toId = toId;
    }

    public static List<Chunk> buildChunks(int count) {
        List<Integer> bucketIds = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            bucketIds.add(i);
        }
        List<Chunk> buckets = new ArrayList<>(count);
        long chunkSize = chunkSize(count);
        for (int id : bucketIds) {
            long fromId = id * chunkSize;
            long toId = fromId + (chunkSize - 1);
            buckets.add(new Chunk(id, fromId, toId));
        }
        return buckets;
    }

    public static long chunkSize(int count) {
        return Long.MAX_VALUE / count + 1;
    }

    @Override
    public String toString() {
        return "Chunk{" +
                "id=" + id +
                ", fromId=" + fromId +
                ", toId=" + toId +
                '}';
    }
}
