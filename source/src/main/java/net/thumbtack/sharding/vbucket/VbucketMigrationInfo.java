package net.thumbtack.sharding.vbucket;

import java.io.IOException;
import java.io.Serializable;

public class VbucketMigrationInfo implements Serializable {

    private static final long serialVersionUID = -6097527897615684857L;
    private int bucketId;
    private int toShardId;

    public VbucketMigrationInfo() {}

    public VbucketMigrationInfo(int bucketId, int toShardId) {
        this.bucketId = bucketId;
        this.toShardId = toShardId;
    }

    public int getBucketId() {
        return bucketId;
    }

    public int getToShardId() {
        return toShardId;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(bucketId);
        out.writeInt(toShardId);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        bucketId = in.readInt();
        toShardId = in.readInt();
    }
}
