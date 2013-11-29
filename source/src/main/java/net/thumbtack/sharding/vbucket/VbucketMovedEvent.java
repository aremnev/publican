package net.thumbtack.sharding.vbucket;

import net.thumbtack.sharding.core.cluster.Event;

import java.io.IOException;

public class VbucketMovedEvent implements Event<VbucketMigrationInfo> {

    private static final long serialVersionUID = 2754942794625233215L;

    public static final long ID = serialVersionUID;

    private VbucketMigrationInfo migrationInfo;

    public VbucketMovedEvent() {}

    public VbucketMovedEvent(int bucketId, int toShardId) {
        migrationInfo = new VbucketMigrationInfo(bucketId, toShardId);
    }

    @Override
    public long getId() {
        return serialVersionUID;
    }

    @Override
    public VbucketMigrationInfo getEventObject() {
        return migrationInfo;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(migrationInfo);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        migrationInfo = (VbucketMigrationInfo) in.readObject();
    }
}
