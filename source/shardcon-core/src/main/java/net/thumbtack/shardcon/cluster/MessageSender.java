package net.thumbtack.shardcon.cluster;

import java.io.Serializable;


public interface MessageSender {

    void sendMessage(Serializable message);
}
