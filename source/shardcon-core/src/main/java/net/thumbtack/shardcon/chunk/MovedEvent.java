package net.thumbtack.shardcon.chunk;

import net.thumbtack.shardcon.core.cluster.Event;

import java.io.IOException;

public class MovedEvent implements Event<MigrationInfo> {

    private static final long serialVersionUID = 2754942794625233215L;

    public static final long ID = serialVersionUID;

    private MigrationInfo migrationInfo;

    public MovedEvent() {}

    public MovedEvent(int bucketId, int toShardId) {
        migrationInfo = new MigrationInfo(bucketId, toShardId);
    }

    @Override
    public long getId() {
        return serialVersionUID;
    }

    @Override
    public MigrationInfo getEventObject() {
        return migrationInfo;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(migrationInfo);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        migrationInfo = (MigrationInfo) in.readObject();
    }
}
