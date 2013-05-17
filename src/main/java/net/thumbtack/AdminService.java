package net.thumbtack;

import net.thumbtack.sharding.core.Shard;

/**
 * Created with IntelliJ IDEA.
 * User: ev
 * Date: 5/17/13
 * Time: 5:05 PM
 * To change this template use File | Settings | File Templates.
 */
public interface AdminService {
    void createNewReplicaBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
    void moveActiveBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
    void checkShard(String shardId) throws BucketServiceException;
    void switchActiveBucket(String dstShardId, int bucketIndex) throws BucketServiceException;
    void addShard(String shardId, Shard shard) throws BucketServiceException;
    void removeShard(String shardId) throws BucketServiceException;
}
