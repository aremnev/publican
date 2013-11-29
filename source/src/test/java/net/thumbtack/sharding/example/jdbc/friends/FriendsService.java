package net.thumbtack.sharding.example.jdbc.friends;

import fj.F3;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

public class FriendsService {

    private FriendsDao dao;

    public FriendsService(FriendsDao dao) {
        this.dao = dao;
    }

    public void addFriend(long userId, long friendId) {
        dao.insert(userId, friendId);
    }

    public void removeFriend(long userId, long friendId) {
        dao.delete(userId, friendId);
    }

    public boolean isFriend(long userId, long friendId) {
        return dao.select(userId, friendId) != null;
    }

    public List<Long> getFriends(long userId) {
        return dao.select(userId);
    }

    @SuppressWarnings("unchecked")
    public List<Long> getMutualFriends(long userId1, long userId2) {
        List<Long> friends1 = getFriends(userId1);
        List<Long> friends2 = getFriends(userId2);
        return new ArrayList<Long>((Collection<Long>) CollectionUtils.intersection(friends1, friends2));
    }

    public int getHops(long userId1, final long userId2) {
        final F3<Set<Long>, Set<Long>, Integer, Integer> search = new F3<Set<Long>, Set<Long>, Integer, Integer>() {
            @Override
            public Integer f(Set<Long> friends, Set<Long> explored, Integer hops) {
                if (friends.isEmpty()) {
                    return Integer.MAX_VALUE;
                }
                if (friends.contains(userId2)) {
                    return hops;
                }
                explored.addAll(friends);
                Set<Long> nextFriends = new HashSet<Long>();
                for (long friendId : friends) {
                    List<Long> friends2 = getFriends(friendId);
                    for (long friendId2 : friends2) {
                        if (! explored.contains(friendId2)) {
                            nextFriends.add(friendId2);
                        }
                    }
                }
                return f(nextFriends, explored, hops + 1);
            }
        };

        Set<Long> friends = new HashSet<Long>(1);
        friends.add(userId1);
        return search.f(friends, new HashSet<Long>(), 0);
    }
}
