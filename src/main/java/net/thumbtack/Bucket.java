package net.thumbtack;

public class Bucket {
    private String shardId;
    private int bucketIndex;

    public Bucket(String shardId, int bucketIndex) {
        this.shardId = shardId;
        this.bucketIndex = bucketIndex;
    }

    public String getShardId() {
        return shardId;
    }

    public int getBucketIndex() {
        return bucketIndex;
    }
}
