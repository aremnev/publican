package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.CommonEntity;
import net.thumbtack.sharding.Dao;

import java.util.List;

public class CommonDao implements Dao<CommonEntity> {
    @Override
    public CommonEntity select(long id) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<CommonEntity> selectAll() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public CommonEntity insert(CommonEntity entity) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean update(CommonEntity entity) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean delete(CommonEntity entity) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean delete(long id) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Void deleteAll() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
