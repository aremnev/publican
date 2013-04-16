package net.thumbtack.sharding;

import net.thumbtack.sharding.common.Dao;
import net.thumbtack.sharding.common.Entity;
import net.thumbtack.sharding.common.StorageServer;
import net.thumbtack.sharding.jdbc.CommonDao;
import net.thumbtack.sharding.jdbc.ShardedDao;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class DaoTest extends ShardingTest {

    private static ShardingSuite shardingSuite;

    private StorageServer server;
    private Dao<Entity> dao;

    private Entity[] testEntities;

    private Random random = new Random(System.currentTimeMillis());

    public DaoTest(StorageServer server, Dao<Entity> dao) {
        this.server = server;
        this.dao = dao;
    }

    @Before
    public void init() throws Exception {
        server.reset();
        testEntities = new Entity[] {
                new Entity(101, "text101", new Date()),
                new Entity(102, "text102", new Date()),
                new Entity(103, "text103", new Date()),
        };
        dao.insert(Arrays.asList(testEntities));
        logger().debug("inserted initial entities");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (shardingSuite != null)
            shardingSuite.stop();
    }

    @Test
    public void insertTest() throws Exception {
        Entity toInsert = new Entity(1, "test", new Date());
        dao.insert(toInsert);
        Entity inserted = dao.select(toInsert.id);
        assertEquals(toInsert, inserted);
    }

    @Test
    public void selectTest() throws Exception {
        Entity toSelect = testEntities[randomEntity()];
        Entity selected = dao.select(toSelect.id);
        assertEquals(toSelect, selected);
    }

    @Test
    public void selectSeveralTest() throws Exception {
        List<Entity> toSelect = Arrays.asList(testEntities[0], testEntities[2]);
        List<Entity> selected = dao.select(Arrays.asList(101L, 103L));
        sortById(selected);
        for (int i = 0; i < toSelect.size(); i++) {
            assertEquals(toSelect.get(i), selected.get(i));
        }
    }

    @Test
    public void selectAllTest() throws Exception {
        List<Entity> selected = dao.selectAll();
        sortById(selected);
        assertEquals(3, selected.size());
        for (int i = 0; i < testEntities.length; i++) {
            assertEquals(testEntities[i], selected.get(i));
        }
    }

    @Test
    public void updateTest() throws Exception {
        Entity toUpdate = testEntities[1];
        toUpdate.setText("FooText");
        Entity notUpdated = dao.select(toUpdate.id);
        assertFalse(toUpdate.equals(notUpdated));
        dao.update(toUpdate);
        Entity updated = dao.select(toUpdate.id);
        assertEquals(toUpdate, updated);
    }

    @Test
    public void deleteTest() throws Exception {
        Entity toDelete = testEntities[1];
        Entity notDeleted = dao.select(toDelete.id);
        assertEquals(toDelete, notDeleted);
        dao.delete(toDelete);
        Entity deleted = dao.select(toDelete.id);
        assertTrue(deleted == null);
    }

    @Test
    public void deleteAllTest() throws Exception {
        List<Entity> notDeleted = dao.selectAll();
        assertFalse(notDeleted.isEmpty());
        dao.deleteAll();
        List<Entity> deleted = dao.selectAll();
        assertTrue(deleted.isEmpty());
    }

    private int randomEntity() {
        return random.nextInt(testEntities.length);
    }

    private void sortById(List<Entity> selected) {
        Collections.sort(selected, new Comparator<Entity>() {
            @Override
            public int compare(Entity e1, Entity e2) {
                long diff = e1.id - e2.id;
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
        });
    }

    @Override
    protected Logger logger() {
        return LoggerFactory.getLogger("test" + dao.getClass().getCanonicalName());
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {
        shardingSuite = new ShardingSuite();
        shardingSuite.start();

        List<Object[]> params = new ArrayList<Object[]>();
        params.add(new Object[] {shardingSuite.jdbcServer, new CommonDao(shardingSuite.jdbcServer.sharding())});
        params.add(new Object[] {shardingSuite.jdbcServer, new ShardedDao(shardingSuite.jdbcServer.sharding())});
        return params;
    }
}
