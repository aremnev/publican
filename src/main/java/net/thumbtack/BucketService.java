package net.thumbtack;

public interface BucketService {
    Result doAction(Action action, long entityId) throws BucketServiceException;
    void moveActiveBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
    void createNewReplicaBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
    void switchActiveBucket(String dstShardId, int bucketIndex) throws BucketServiceException;
    void addShard(String shardId) throws BucketServiceException;
    void removeShard(String shardId) throws BucketServiceException;
    Result doReadAction(Action action) throws BucketServiceException;
    void checkShard(String shardId) throws BucketServiceException;
}
