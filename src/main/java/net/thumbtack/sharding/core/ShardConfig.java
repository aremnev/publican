package net.thumbtack.sharding.core;

public class ShardConfig {

    private long id;

    public ShardConfig() {}

    public ShardConfig(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
