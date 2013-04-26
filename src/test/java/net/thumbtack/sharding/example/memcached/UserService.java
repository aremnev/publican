package net.thumbtack.sharding.example.memcached;


public class UserService {
    private UserDao userDao;

    public UserService(UserDao userDao) {
        this.userDao = userDao;
    }

    public void insert(final long userId, final User user) {
        userDao.insert(userId, user);
    }

    public User select(final long userId) {
        return userDao.select(userId);
    }

    public boolean delete(final long userId) {
        return userDao.delete(userId);
    }
}
