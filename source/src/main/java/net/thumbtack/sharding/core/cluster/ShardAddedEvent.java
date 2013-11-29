package net.thumbtack.sharding.core.cluster;

import net.thumbtack.sharding.core.Shard;

import java.io.IOException;

public class ShardAddedEvent implements Event<Shard> {

    private static final long serialVersionUID = 6516336392859700813L;

    public static final long ID = serialVersionUID;

    private Shard shard;

    public ShardAddedEvent() {}

    public ShardAddedEvent(Shard shard) {
        this.shard = shard;
    }

    @Override
    public long getId() {
        return serialVersionUID;
    }

    @Override
    public Shard getEventObject() {
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
