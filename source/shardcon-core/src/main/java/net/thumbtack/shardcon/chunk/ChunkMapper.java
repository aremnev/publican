package net.thumbtack.shardcon.chunk;

import net.thumbtack.shardcon.core.KeyMapper;

import java.util.HashMap;
import java.util.Map;

public class ChunkMapper implements KeyMapper {

    private final Map<Integer, Integer> bucketToShard;
    private final KeyMapper idToBucket;
    private volatile KeyMapper internalMapper;

    private KeyMapper basicMapper = new KeyMapper() {
        @Override
        public int shard(long key) {
            return map(key);
        }
    };

    private KeyMapper safeMapper = new KeyMapper() {
        @Override
        public int shard(long key) {
            synchronized (bucketToShard) { return map(key); }
        }
    };

    public ChunkMapper(Map<Integer, Integer> bucketToShard, KeyMapper idToBucket) {
        this.bucketToShard = new HashMap<>(bucketToShard);
        this.idToBucket = idToBucket;
        internalMapper = basicMapper;
    }

    @Override
    public int shard(long key) {
        return internalMapper.shard(key);
    }

    public void moveBucket(int bucket, int movedTo) {
        if (bucketToShard.get(bucket) != movedTo) {
            synchronized (bucketToShard) {
                internalMapper = safeMapper;
                bucketToShard.remove(bucket);
                bucketToShard.put(bucket, movedTo);
                internalMapper = basicMapper;
            }
        }
    }

    private int map(long key) {
        int bucket = idToBucket.shard(key);
        return bucketToShard.get(bucket);
    }
}
