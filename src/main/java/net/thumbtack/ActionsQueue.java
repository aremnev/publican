package net.thumbtack;

/**
 * Created with IntelliJ IDEA.
 * User: ev
 * Date: 5/8/13
 * Time: 4:25 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ActionsQueue {
    public Action pop();
    public int count();
}
