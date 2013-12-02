package net.thumbtack.shardcon.core;

import net.thumbtack.shardcon.core.Shard;

import java.io.IOException;
import java.io.Serializable;

public class NewShardEvent implements Serializable {

    private static final long serialVersionUID = 6516336392859700813L;

    private Shard shard;

    public NewShardEvent() {}

    public NewShardEvent(Shard shard) {
        this.shard = shard;
    }

    public Shard getShard() {
        return shard;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(shard);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        shard = (Shard) in.readObject();
    }
}
