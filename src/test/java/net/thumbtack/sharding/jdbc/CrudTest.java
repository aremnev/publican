package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.core.Connection;
import net.thumbtack.sharding.core.QueryClosure;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static net.thumbtack.sharding.core.Sharding.*;
import static org.junit.Assert.assertEquals;

public class CrudTest extends JdbcTest {

    @Test
    public void insertTest() throws Exception {
        final MutableInt inserted = new MutableInt(0);
        sharding.execute(UPDATE_ALL_SHARDS, new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "INSERT INTO `common` (`id`,`text`) VALUES (1,'test')";
                int ins = sqlConn.createStatement().executeUpdate(sql);
                assertEquals(1, ins);
                synchronized (inserted) {
                    inserted.setValue(inserted.getValue() + ins);
                }
                return ins > 0;
            }
        });
        assertEquals(2, inserted.getValue().intValue());
    }
}