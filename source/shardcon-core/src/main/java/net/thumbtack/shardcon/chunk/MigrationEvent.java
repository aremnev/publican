package net.thumbtack.shardcon.chunk;

import java.io.IOException;
import java.io.Serializable;

public class MigrationEvent implements Serializable {

    private static final long serialVersionUID = 2754942794625233215L;

    private int chunkId;
    private int toShardId;

    private long newVersion;

    public MigrationEvent() {}

    public MigrationEvent(int chunkId, int toShardId, long newVersion) {
        this.chunkId = chunkId;
        this.toShardId = toShardId;
        this.newVersion = newVersion;
    }

    public int getChunkId() {
        return chunkId;
    }

    public int getToShardId() {
        return toShardId;
    }

    public long getNewVersion() {
        return newVersion;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(chunkId);
        out.writeInt(toShardId);
        out.writeLong(newVersion);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        chunkId = in.readInt();
        toShardId = in.readInt();
        newVersion = in.readLong();
    }
}
