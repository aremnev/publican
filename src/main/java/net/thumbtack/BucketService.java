package net.thumbtack;

import net.thumbtack.sharding.core.Shard;

public interface BucketService {

    // Clients.
    Result doAction(Action action, long entityId) throws BucketServiceException;
    Result doReadAction(Action action) throws BucketServiceException;

    // Admin.
    void addShard(String shardId, Shard shard) throws BucketServiceException;
    void removeShard(String shardId) throws BucketServiceException;
    void checkShard(String shardId) throws BucketServiceException;

    void moveActiveBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
    void createNewReplicaBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
    void switchActiveBucket(String dstShardId, int bucketIndex) throws BucketServiceException;
}
