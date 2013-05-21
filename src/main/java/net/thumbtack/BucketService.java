package net.thumbtack;

import java.util.Collection;
import java.util.Iterator;

public interface BucketService {

    // for WriteActionStrategy
    Result doAction(String shardId, Action action) throws BucketServiceException;

    // For ReadActionStrategy
    Iterator<Integer> getAllBucketIndexIterator();

    int mapEntityIdToBucketIndex(Long entityId);
    void setBucketState(Bucket bucket, BucketState bucketState);
    void addReplicaBucket(Bucket newReplicaBucket);

    // Admin.
    BucketState retrieveBucketState(Bucket bucket);
    int retrieveBucketUsageCount(Bucket bucket);
    Collection<Bucket> findActiveBucketsOnShard(String shardId);
    void removeReplicaBucket(Bucket dstBucket);
    void waitUntilSomeoneIsUsingBucket(Bucket bucket) throws BucketServiceException;
    Bucket findActiveBucket(int bucketIndex);


    void lockBucketIndex(int bucketIndex) throws BucketServiceException;
    Collection<String> findReplicaShardIds(int bucketIndex) throws BucketServiceException;
    void unlockBucketIndex(int bucketIndex) throws BucketServiceException;

    long retrieveLastAcceptedActionId(Bucket bucket) throws BucketServiceException;

    void updateLastAcceptedAction(Bucket bucket, Long actionId) throws BucketServiceException;
}
