package net.thumbtack.sharding.example.jdbc.friends;

import net.thumbtack.sharding.Dao;
import net.thumbtack.sharding.ShardingFacade;

import java.util.List;

public class FriendshipDao implements Dao<Friendship> {

    private ShardingFacade sharding;

    public FriendshipDao(ShardingFacade sharding) {
        this.sharding = sharding;
    }

    @Override
    public Friendship select(long id) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Friendship> select(List<Long> ids) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Friendship> selectAll() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Friendship insert(Friendship entity) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Friendship> insert(List<Friendship> entities) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean update(Friendship entity) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean delete(Friendship entity) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean delete(long id) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean deleteAll() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
