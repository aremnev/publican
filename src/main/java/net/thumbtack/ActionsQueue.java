package net.thumbtack;

public interface ActionsQueue {
    Action pop() throws ActionsQueueException;
    int count();
    boolean isEmpty();
}
