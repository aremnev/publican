package net.thumbtack.sharding.example.memcached;

import net.thumbtack.sharding.test.ShardingSuite;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.Assert.*;

public class UserServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(UserServiceTest.class);

    private UserService userService;
    private UserDao userDao;
    private UserStorage userStorage;

    public UserServiceTest() throws Exception {
        userStorage = new UserStorage();
        userDao = new UserDao(userStorage.sharding());
        userService = new UserService(userDao);
    }

    @Before
    public void init() throws Exception {
        userStorage.reset();
        userDao.insert(1, new User("user1", 11));
        userDao.insert(2, new User("user2", 22));
        userDao.insert(3, new User("user3", 33));

        logger.debug("inserted initial entities");
    }

    @Test
    public void selectTest() {
        userService.insert(1, new User("user1", 11));
        User user = userService.select(1);
        assertNotNull(user);
    }

    @Test
    public void updateTest() {
        String userName1 = "user1";
        String userName2 = "user2";
        userService.insert(1, new User(userName1, 11));
        User user1 = userService.select(1);
        assertEquals(user1.getName(), userName1);
        userService.insert(1, new User("user2", 22));
        User user2 = userService.select(1);
        assertEquals(user2.getName(), userName2);
    }

    @Test
    public void deleteTest() {
        userService.insert(1, new User("user1", 11));
        User user = userService.select(1);
        assertNotNull(user);
        userService.delete(1);
        user = userService.select(1);
        assertNull(user);
    }

    @BeforeClass
    public static void startEmbeddedMemcached() throws Exception {
        ShardingSuite shardingSuite = ShardingSuite.getInstance();
        shardingSuite.start();
    }
}
