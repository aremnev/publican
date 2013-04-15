package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.CommonEntity;
import net.thumbtack.sharding.core.Connection;
import net.thumbtack.sharding.core.QueryClosure;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static net.thumbtack.sharding.core.Sharding.*;
import static org.junit.Assert.assertEquals;

public class CrudCommonTest extends JdbcTest {

    private static final Logger logger = LoggerFactory.getLogger(CrudCommonTest.class);

    @Override
    protected Logger logger() {
        return logger;
    }

    private static final CommonEntity[] testEntities = new CommonEntity[] {
            new CommonEntity(101, "text101", new Date()),
            new CommonEntity(102, "text102", new Date()),
            new CommonEntity(103, "text103", new Date()),
    };

    private static Random random = new Random(System.currentTimeMillis());

    @Before
    public void init() throws Exception {
        sharding.execute(UPDATE_ALL_SHARDS, insertClosure(testEntities));
        logger.debug("inserted initial entities");
    }

    @Test
    public void insertTest() throws Exception {
        final AtomicInteger inserted = new AtomicInteger(0);
        final CommonEntity toInsert = new CommonEntity(1, "test", new Date());
        sharding.execute(UPDATE_ALL_SHARDS, new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = insertStr(toInsert);
                int ins = sqlConn.createStatement().executeUpdate(sql);
                inserted.set(inserted.get() + ins);
                assertEquals(1, ins);
                return ins > 0;
            }
        });
        assertEquals(2, inserted.get());
    }

    @Test
    public void selectTest() throws Exception {
        final CommonEntity toSelect = testEntities[randomCommonEntity()];
        CommonEntity selected = sharding.execute(SELECT_SHARD, selectByIdClosure(toSelect.id));
        assertEquals(toSelect, selected);
    }

    @Test
    public void selectAnyShardTest() throws Exception {
        final CommonEntity toSelect = testEntities[randomCommonEntity()];
        CommonEntity selected = sharding.execute(SELECT_ANY_SHARD, selectByIdClosure(toSelect.id));
        assertEquals(toSelect, selected);
    }

    @Test
    public void selectAllTest() throws Exception {
        List<CommonEntity> selected = sharding.execute(SELECT_ANY_SHARD, selectAllClosure());
        assertEquals(3, selected.size());
        for (int i = 0; i < testEntities.length; i++) {
            assertEquals(testEntities[i], selected.get(i));
        }
    }

    private int randomCommonEntity() {
        return random.nextInt(testEntities.length);
    }

    private QueryClosure<CommonEntity> selectByIdClosure(final long id) {
        return new QueryClosure<CommonEntity>() {
            @Override
            public CommonEntity call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "SELECT * FROM `common` WHERE `id` = "+ id +";";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                resultSet.next();
                return new CommonEntity(
                        resultSet.getLong(1),
                        resultSet.getString(2),
                        Timestamp.valueOf(resultSet.getString(3))
                );
            }
        };
    }

    private QueryClosure<List<CommonEntity>> selectAllClosure() {
        return new QueryClosure<List<CommonEntity>>() {
            @Override
            public List<CommonEntity> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "SELECT * FROM `common` ORDER BY `id`;";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                List<CommonEntity> result = new ArrayList<CommonEntity>();
                while (resultSet.next()) {
                    result.add(new CommonEntity(
                            resultSet.getLong(1),
                            resultSet.getString(2),
                            Timestamp.valueOf(resultSet.getString(3))
                    ));
                }
                return result;
            }
        };
    }

    private QueryClosure<Boolean> insertClosure(final CommonEntity[] entities) {
        return new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "";
                for (CommonEntity CommonEntity : entities) {
                    sql += insertStr(CommonEntity);
                }
                int ins = sqlConn.createStatement().executeUpdate(sql);
                return ins > 0;
            }
        };
    }

    private String insertStr(CommonEntity CommonEntity) {
        Timestamp time  = new Timestamp(CommonEntity.date.getTime());
        return "INSERT INTO `common` (`id`,`text`, `date`) VALUES (" + CommonEntity.id + ",'" + CommonEntity.text + "', '" + time.toString() + "');";
    }
}

