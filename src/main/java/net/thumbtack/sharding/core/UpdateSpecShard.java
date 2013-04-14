package net.thumbtack.sharding.core;

import java.util.List;

public class UpdateSpecShard extends Query {

    @Override
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        U result = null;
        Connection connection = shards.get(0);
        connection.open();
        try {
            result = closure.call(connection);
            connection.commit();
        } catch (RuntimeException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.close();
        }
        return result;
    }
}
