package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.Entity;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class CommonDaoTest extends JdbcTest {

    private static final Logger logger = LoggerFactory.getLogger(CommonDaoTest.class);

    @Override
    protected Logger logger() {
        return logger;
    }

    private static final Entity[] testEntities = new Entity[] {
            new Entity(101, "text101", new Date()),
            new Entity(102, "text102", new Date()),
            new Entity(103, "text103", new Date()),
    };

    private static Random random = new Random(System.currentTimeMillis());

    private static CommonDao dao = new CommonDao(sharding);

    @Before
    public void init() throws Exception {
        dao.insert(Arrays.asList(testEntities));
        logger.debug("inserted initial entities");
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
        for (int i = 0; i < toSelect.size(); i++) {
            assertEquals(toSelect.get(i), selected.get(i));
        }
    }

    @Test
    public void selectAllTest() throws Exception {
        List<Entity> selected = dao.selectAll();
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
}

