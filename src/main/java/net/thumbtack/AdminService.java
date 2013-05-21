package net.thumbtack;

import net.thumbtack.sharding.core.Shard;

public interface AdminService {
    void createNewReplicaBucketOnShard(String dstShardId, int bucketIndex) throws BucketServiceException;
    void moveActiveBucketToShard(String dstShardId, int bucketIndex) throws BucketServiceException;
    void checkShard(String shardId) throws BucketServiceException;
    void switchActiveBucket(String dstShardId, int bucketIndex) throws BucketServiceException;
    void addShard(String shardId, Shard shard) throws BucketServiceException;
    void removeShard(String shardId) throws BucketServiceException;
    void incrementalSync(Bucket bucket, long lastActionId) throws BucketServiceException;
}
