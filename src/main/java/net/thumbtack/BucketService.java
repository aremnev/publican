package net.thumbtack;

public interface BucketService {
    void doAction(Action action) throws BucketServiceException;
    void moveActiveBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
    void createNewReplicaBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
    void switchActiveBucket(String dstShardId, int bucketIndex) throws BucketServiceException;
    public void addShard(String shardId) throws BucketServiceException;
    public void removeShard(String shardId) throws BucketServiceException;
}
