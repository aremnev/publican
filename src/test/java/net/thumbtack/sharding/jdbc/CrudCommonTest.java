package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.Entity;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import static org.junit.Assert.assertEquals;

public class CrudCommonTest extends JdbcTest {

    private static final Logger logger = LoggerFactory.getLogger(CrudCommonTest.class);

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
        final Entity toSelect = testEntities[randomCommonEntity()];
        Entity selected = dao.select(toSelect.id);
        assertEquals(toSelect, selected);
    }

    @Test
    public void selectAllTest() throws Exception {
        List<Entity> selected = dao.selectAll();
        assertEquals(3, selected.size());
        for (int i = 0; i < testEntities.length; i++) {
            assertEquals(testEntities[i], selected.get(i));
        }
    }

    private int randomCommonEntity() {
        return random.nextInt(testEntities.length);
    }
}

