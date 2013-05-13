package net.thumbtack;

public interface BucketService {
    public void doAction(Action action) throws BucketServiceException;
    void moveActiveBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException;
}
