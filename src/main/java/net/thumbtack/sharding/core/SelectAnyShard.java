package net.thumbtack.sharding.core;

import net.thumbtack.helper.Util;

import java.util.List;
import java.util.Random;

public class SelectAnyShard extends Query {

    private Random random;

    public SelectAnyShard(Random random) {
        this.random = random;
    }

    @Override
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        U result = null;
        Connection connection = Util.getRandom(random, shards);
        connection.open();
        try {
            result = closure.call(connection);
        } finally {
            connection.close();
        }
        return result;
    }
}
