package net.thumbtack.sharding.test.common;

import net.thumbtack.sharding.Dao;

import java.util.List;

public interface EntityDao extends Dao<Entity> {
    @Override
    Entity select(long id);

    @Override
    List<Entity> select(List<Long> ids);

    @Override
    List<Entity> selectAll();

    @Override
    Entity insert(Entity entity);

    @Override
    List<Entity> insert(List<Entity> entities);

    @Override
    boolean update(Entity entity);

    @Override
    boolean delete(Entity entity);

    @Override
    boolean delete(long id);

    @Override
    boolean deleteAll();
}
