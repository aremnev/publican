package net.thumbtack.sharding.example.jdbc.friends;

public class Friendship {

    private long userId;
    private long friendId;

    public Friendship() {
    }

    public Friendship(long userId, long friendId) {
        this.userId = userId;
        this.friendId = friendId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getFriendId() {
        return friendId;
    }

    public void setFriendId(long friendId) {
        this.friendId = friendId;
    }
}
