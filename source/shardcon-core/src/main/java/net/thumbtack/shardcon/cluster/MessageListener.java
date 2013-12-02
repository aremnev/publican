package net.thumbtack.shardcon.cluster;

import java.io.Serializable;

/**
 * Handles any serializable message.
 */
public interface MessageListener {

    void onMessage(Serializable message);
}
