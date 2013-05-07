package net.thumbtack.sharding.vbucket;

public class Vbucket {

    public final int id;
    public final long idFrom;
    public final long idTo;

    public Vbucket(int id, long idFrom, long idTo) {
        this.id = id;
        this.idFrom = idFrom;
        this.idTo = idTo;
    }
}
