package net.thumbtack.sharding.example.jdbc.friends;

import au.com.bytecode.opencsv.CSVReader;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;
import static net.thumbtack.helper.Util.getResourceAsReader;

public class FriendsServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(FriendsServiceTest.class);

    private FriendsService friendsService;
    private FriendsDao dao;
    private FriendsStorage storage;

    public FriendsServiceTest() throws Exception {
        storage = new FriendsStorage();
        dao = new FriendsDao(storage.sharding());
        friendsService = new FriendsService(dao);
    }

    @Before
    public void init() throws Exception {
        storage.reset();

        CSVReader csvReader = new CSVReader(getResourceAsReader("friends.txt"), ',');
        List<String[]> lines = csvReader.readAll();
        List<Long> userIds = new ArrayList<Long>(lines.size());
        List<Long> friendIds = new ArrayList<Long>(lines.size());
        for (String[] line : lines) {
            userIds.add(Long.parseLong(line[0]));
            friendIds.add(Long.parseLong(line[1]));
        }
        dao.insert(userIds, friendIds);

        logger.debug("inserted initial entities");
    }

    @Test
    public void isFriendTest() {
        assertTrue(friendsService.isFriend(1, 2));
        assertFalse(friendsService.isFriend(1, 9));
    }

    @Test
    public void getFriendsTest() {
        List<Long> friends = friendsService.getFriends(1);
        assertEquals(8, friends.size());
        assertList("2, 3, 4, 5, 6, 7, 8, 15", friends);
    }

    @Test
    public void addFriendTest() {
        assertFalse(friendsService.isFriend(1, 9));
        friendsService.addFriend(1, 9);
        assertTrue(friendsService.isFriend(1, 9));
    }

    @Test
    public void removeFriendTest() {
        assertTrue(friendsService.isFriend(1, 2));
        friendsService.removeFriend(1, 2);
        assertFalse(friendsService.isFriend(1, 2));
    }

    @Test
    public void getMutualFriendsTest() {
        assertList("3, 4, 5, 6, 7, 8, 15", friendsService.getMutualFriends(1, 2));
        assertList("", friendsService.getMutualFriends(1, 13));
    }

    @Test
    public void getHopsTest() {
        assertEquals(0, friendsService.getHops(1, 1));
        assertEquals(1, friendsService.getHops(1, 2));
        assertEquals(2, friendsService.getHops(1, 9));
        assertEquals(Integer.MAX_VALUE, friendsService.getHops(1, 13));
    }

    private static void assertList(String s, List<Long> list) {
        if (list == null) {
            assertTrue(s == null);
            return;
        }

        Collections.sort(list, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                long diff = o1 - o2;
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
        });
        StringBuilder ls = new StringBuilder();
        for (long l : list) {
            ls.append(l).append(", ");
        }
        if (ls.length() > 0) {
            ls.delete(ls.length() - 2, ls.length());
        }
        assertEquals(s, ls.toString());
    }
}
