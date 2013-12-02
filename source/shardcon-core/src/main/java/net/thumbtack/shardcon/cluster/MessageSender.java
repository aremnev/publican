package net.thumbtack.shardcon.cluster;

import java.io.Serializable;

/**
 * Sends any serializable message.
 */
public interface MessageSender {

    void sendMessage(Serializable message);
}
