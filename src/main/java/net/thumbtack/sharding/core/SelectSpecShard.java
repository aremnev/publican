package net.thumbtack.sharding.core;


import java.util.List;

public class SelectSpecShard extends Query {

    @Override
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) throws Exception {
        U result = null;
        Connection connection = shards.get(0);
        connection.open();
        try {
            result = closure.call(connection);
        } finally {
            connection.close();
        }
        return result;
    }
}
