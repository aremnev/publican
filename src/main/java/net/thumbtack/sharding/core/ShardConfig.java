package net.thumbtack.sharding.core;

/**
 * The shard configuration.
 */
public class ShardConfig {

    private int id;

    /**
     * Default constructor.
     */
    public ShardConfig() {}

    /**
     * Constructor.
     * @param id The shardId.
     */
    public ShardConfig(int id) {
        this.id = id;
    }

    /**
     * Gets the shard id.
     * @return The shard id.
     */
    public int getId() {
        return id;
    }

    /**
     * Sets the shard id.
     * @param id The shard id.
     */
    public void setId(int id) {
        this.id = id;
    }
}
