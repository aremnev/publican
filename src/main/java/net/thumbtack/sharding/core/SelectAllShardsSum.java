package net.thumbtack.sharding.core;

import java.util.List;

public class SelectAllShardsSum extends Query {

    @Override
    @SuppressWarnings("unchecked")
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) throws Exception {
        Integer result = 0;
        for (final Connection connection : shards) {
            connection.open();
            try {
                Integer res = (Integer) closure.call(connection);
                if (res != null) {
                    result += res;
                }
            } finally {
                connection.close();
            }
        }
        return (U) result;
    }
}
