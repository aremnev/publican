package net.thumbtack;

public interface ActionsQueue {
    public Action pop() throws ActionsQueueException;
    public int count();
    boolean isEmpty();
}
