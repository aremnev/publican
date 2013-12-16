package net.thumbtack.shardcon.core;

import java.io.IOException;
import java.io.Serializable;

public class NewShardEvent implements Serializable {

    private static final long serialVersionUID = 6516336392859700813L;

    private Shard shard;

    private long newVersion;

    public NewShardEvent() {}

    public NewShardEvent(Shard shard, long newVersion) {
        this.shard = shard;
        this.newVersion = newVersion;
    }

    public Shard getShard() {
        return shard;
    }

    public long getNewVersion() {
        return newVersion;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(shard);
        out.writeLong(newVersion);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        shard = (Shard) in.readObject();
        newVersion = in.readLong();
    }
}
