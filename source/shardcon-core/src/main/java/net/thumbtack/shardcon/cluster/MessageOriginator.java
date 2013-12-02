package net.thumbtack.shardcon.cluster;

/**
 * Initiates messages and sends them via messageSender.sendMessage().
 */
public interface MessageOriginator {

    void setMessageSender(MessageSender messageSender);
}
