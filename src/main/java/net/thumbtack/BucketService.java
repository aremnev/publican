package net.thumbtack;

import net.thumbtack.sharding.core.Shard;

import java.util.Collection;
import java.util.Iterator;

public interface BucketService {

    // Clients.
    Result doAction(Action action, long entityId, ActionStrategy actionStrategy) throws BucketServiceException;
    Result doReadAction(Action action, ActionStrategy actionStrategy) throws BucketServiceException;

    // for WriteActionStrategy
    Result doAction(String shardId, Action action) throws BucketServiceException;

    // For ReadActionStrategy
    Iterator<Integer> getAllBucketIndexIterator();

    int mapEntityIdToBucketIndex(Long entityId);

    // Admin.
    void addShard(String shardId, Shard shard) throws BucketServiceException;
    void removeShard(String shardId) throws BucketServiceException;
    void checkShard(String shardId) throws BucketServiceException;

    void moveActiveBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
    void createNewReplicaBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
    void switchActiveBucket(String dstShardId, int bucketIndex) throws BucketServiceException;

    void lockBucketIndex(int bucketIndex) throws BucketServiceException;
    Collection<String> findReplicaShardIds(int bucketIndex) throws BucketServiceException;
    public void unlockBucketIndex(int bucketIndex) throws BucketServiceException;
}
