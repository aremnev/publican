package net.thumbtack.sharding.test;

import fj.F;
import net.thumbtack.sharding.Dao;
import net.thumbtack.sharding.example.jdbc.friends.User;
import net.thumbtack.sharding.example.jdbc.friends.UserDao;
import net.thumbtack.sharding.test.common.Entity;
import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.test.jdbc.CommonDao;
import net.thumbtack.sharding.test.jdbc.ShardedDao;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static net.thumbtack.helper.Util.*;

@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class DaoTest extends ShardingTest {

    private static ShardingSuite shardingSuite;

    private Storage server;
    private Dao dao;

    private Object[] inserted;
    private Object notInserted;
    private F<Object, Long> fId;

    private Random random = new Random(System.currentTimeMillis());

    public DaoTest(
            Storage server,
            Dao dao,
            Object[] inserted,
            Object notInserted,
            F<Object, Long> fId) {
        this.server = server;
        this.dao = dao;
        this.inserted = inserted;
        this.notInserted = notInserted;
        this.fId = fId;
    }

    @Before
    public void init() throws Exception {
        server.reset();

        dao.insert(Arrays.asList(inserted));
        logger().debug("inserted initial entities");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (shardingSuite != null)
            shardingSuite.stop();
    }

    @Test
    public void insertTest() throws Exception {
        Object toInsert = notInserted;
        dao.insert(toInsert);
        Object inserted = dao.select(fId.f(toInsert));
        assertEquals(toInsert, inserted);
    }

    @Test
    public void selectTest() throws Exception {
        Object toSelect = inserted[randomEntity()];
        Object selected = dao.select(fId.f(toSelect));
        assertEquals(toSelect, selected);
    }

    @Test
    public void selectSeveralTest() throws Exception {
        List<Object> toSelect = Arrays.asList(inserted[0], inserted[2]);
        List<Object> selected = dao.select(Arrays.asList(fId.f(toSelect.get(0)), fId.f(toSelect.get(1))));
        sortById(selected);
        for (int i = 0; i < toSelect.size(); i++) {
            assertEquals(toSelect.get(i), selected.get(i));
        }
    }

    @Test
    public void selectAllTest() throws Exception {
        List<Object> selected = dao.selectAll();
        sortById(selected);
        assertEquals(3, selected.size());
        for (int i = 0; i < inserted.length; i++) {
            assertEquals(inserted[i], selected.get(i));
        }
    }

    @Test
    public void updateTest() throws Exception {
        Object toUpdate = inserted[1];
        changeSomeTextField(toUpdate);
        Object notUpdated = dao.select(fId.f(toUpdate));
        assertFalse(toUpdate.equals(notUpdated));
        dao.update(toUpdate);
        Object updated = dao.select(fId.f(toUpdate));
        assertEquals(toUpdate, updated);
    }

    @Test
    public void updateSeveralTest() throws Exception {
        List<Object> toUpdate = Arrays.asList(inserted[0], inserted[2]);
        changeSomeTextField(toUpdate.get(0));
        changeSomeTextField(toUpdate.get(1));
        List<Object> notUpdated = dao.select(Arrays.asList(fId.f(toUpdate.get(0)), fId.f(toUpdate.get(1))));
        sortById(notUpdated);
        for (int i = 0; i < toUpdate.size(); i++) {
            assertFalse(toUpdate.get(i).equals(notUpdated.get(i)));
        }

        dao.update(toUpdate);
        List<Object> updated = dao.select(Arrays.asList(fId.f(toUpdate.get(0)), fId.f(toUpdate.get(1))));
        sortById(updated);
        for (int i = 0; i < toUpdate.size(); i++) {
            assertEquals(toUpdate.get(i), updated.get(i));
        }
    }

    @Test
    public void deleteTest() throws Exception {
        List<Object> toDelete = Arrays.asList(inserted[0], inserted[2]);
        List<Object> notDeleted = dao.select(Arrays.asList(fId.f(toDelete.get(0)), fId.f(toDelete.get(1))));
        for (int i = 0; i < toDelete.size(); i++) {
            assertEquals(toDelete.get(i), notDeleted.get(i));
        }
        dao.delete(map(toDelete, fId));
        List<Object> deleted = dao.select(Arrays.asList(fId.f(toDelete.get(0)), fId.f(toDelete.get(1))));
        assertTrue(deleted.isEmpty());
    }

    @Test
    public void deleteSeveralTest() throws Exception {
        Object toDelete = inserted[1];
        Object notDeleted = dao.select(fId.f(toDelete));
        assertEquals(toDelete, notDeleted);
        dao.delete(toDelete);
        Object deleted = dao.select(fId.f(toDelete));
        assertTrue(deleted == null);
    }

    @Test
    public void deleteAllTest() throws Exception {
        List<Object> notDeleted = dao.selectAll();
        assertFalse(notDeleted.isEmpty());
        dao.deleteAll();
        List<Object> deleted = dao.selectAll();
        assertTrue(deleted.isEmpty());
    }

    private int randomEntity() {
        return random.nextInt(inserted.length);
    }

    private void sortById(List<Object> selected) {
        Collections.sort(selected, new Comparator<Object>() {
            @Override
            public int compare(Object e1, Object e2) {
                long diff = fId.f(e1) - fId.f(e2);
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
        });
    }

    private void changeSomeTextField(Object toUpdate) {
        Field[] fields = getAllFields(toUpdate.getClass());
        for (Field field : fields) {
            if (field.getType().equals(String.class)) {
                field.setAccessible(true);
                try {
                    field.set(toUpdate, UUID.randomUUID().toString());
                } catch (IllegalAccessException ignored) {}
                break;
            }
        }
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

        F<Object, Long> entityId = new F<Object, Long>() {
            @Override
            public Long f(Object o) {
                return ((Entity) o).id;
            }
        };

        params.add(new Object[] {shardingSuite.jdbcStorageAsync, new CommonDao(shardingSuite.jdbcStorageAsync.sharding()), createInserted(), createNotInserted(), entityId});
        params.add(new Object[] {shardingSuite.jdbcStorageAsync, new CommonDao(shardingSuite.jdbcStorageAsync.sharding()), createInserted(), createNotInserted(), entityId});
        params.add(new Object[] {shardingSuite.jdbcStorageSync, new ShardedDao(shardingSuite.jdbcStorageSync.sharding()), createInserted(), createNotInserted(), entityId});
        params.add(new Object[] {shardingSuite.jdbcStorageSync, new ShardedDao(shardingSuite.jdbcStorageSync.sharding()), createInserted(), createNotInserted(), entityId});

        User[] insertedUsers = new User[] {
                new User(1, "user1@gmail.com", "user1", new Date()),
                new User(2, "user2@gmail.com", "user2", new Date()),
                new User(3, "user3@gmail.com", "user3", new Date())
        };

        User notInsertedUser = new User(4, "user4@gmail.com", "user4", new Date());

        F<Object, Long> userIdId = new F<Object, Long>() {
            @Override
            public Long f(Object o) {
                return ((User) o).getId();
            }
        };

        params.add(new Object[] {shardingSuite.friendsStorage, new UserDao(shardingSuite.friendsStorage.sharding()), insertedUsers, notInsertedUser, userIdId});

        return params;
    }

    private static Entity createNotInserted() {
        return new Entity(104, "text104", new Date());
    }

    private static Entity[] createInserted() {
        return new Entity[] {
                    new Entity(101, "text101", new Date()),
                    new Entity(102, "text102", new Date()),
                    new Entity(103, "text103", new Date())
            };
    }
}
