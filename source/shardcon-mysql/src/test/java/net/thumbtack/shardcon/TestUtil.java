package net.thumbtack.shardcon;

import fj.F;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestUtil {

    public static long generateUID() {
        long uid;
        do {
            // we need to guarantee not to return 0, because it is used
            // to designate non-existing entities
            uid = Math.abs(UUID.randomUUID().getMostSignificantBits());
        } while (uid == 0);
        return uid;
    }

    public static <V> List<V> parseEntities(ResultSet resultSet, F<ResultSet, V> parseEntity) throws SQLException {
        List<V> result = new ArrayList<V>();
        while (resultSet.next()) {
            result.add(parseEntity.f(resultSet));
        }
        return result;
    }
}
