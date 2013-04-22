package net.thumbtack.sharding;

import fj.F;
import net.thumbtack.sharding.test.common.Entity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class SqlUtil {

    public static <V> List<V> parseEntities(ResultSet resultSet, F<ResultSet, V> parseEntity) throws SQLException {
        List<V> result = new ArrayList<V>();
        while (resultSet.next()) {
            result.add(parseEntity.f(resultSet));
        }
        return result;
    }
}
