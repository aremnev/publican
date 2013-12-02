package net.thumbtack.shardcon.chunk;

import java.io.IOException;
import java.io.Serializable;

public class MigrationEvent implements Serializable {

    private static final long serialVersionUID = 2754942794625233215L;

    private int chunkId;
    private int toShardId;

    public MigrationEvent() {}

    public MigrationEvent(int chunkId, int toShardId) {
        this.chunkId = chunkId;
        this.toShardId = toShardId;
    }

    public int getChunkId() {
        return chunkId;
    }

    public int getToShardId() {
        return toShardId;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(chunkId);
        out.writeInt(toShardId);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        chunkId = in.readInt();
        toShardId = in.readInt();
    }
}
