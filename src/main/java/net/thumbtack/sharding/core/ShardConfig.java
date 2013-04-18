package net.thumbtack.sharding.core;

/**
 * The shard configuration.
 */
public class ShardConfig {

    private long id;

    /**
     * Default constructor.
     */
    public ShardConfig() {}

    /**
     * Constructor.
     * @param id The shardId.
     */
    public ShardConfig(long id) {
        this.id = id;
    }

    /**
     * Get the shard id.
     * @return The shard id.
     */
    public long getId() {
        return id;
    }

    /**
     * Set the shard id.
     * @param id The shard id.
     */
    public void setId(long id) {
        this.id = id;
    }
}
