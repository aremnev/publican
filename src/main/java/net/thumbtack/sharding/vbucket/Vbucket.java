package net.thumbtack.sharding.vbucket;

public class Vbucket {

    public final int id;
    public final long fromId;
    public final long toId;

    public Vbucket(int id, long fromId, long toId) {
        this.id = id;
        this.fromId = fromId;
        this.toId = toId;
    }
}
