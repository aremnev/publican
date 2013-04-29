package net.thumbtack.sharding.core.map;

/**
 * State of the bucket on the shard.
 */
public enum VbucketState {
    /**
     * This server is servicing all requests for this vbucket.
     */
    active,
    /**
     * This server is not in any way responsible for this vbucket.
     */
    dead,
    /**
     * No client requests are handled for this vbucket, but it can receive replication commands.
     */
    replica,
    /**
     * This server will block all requests for this vbucket.
     */
    pending
}
