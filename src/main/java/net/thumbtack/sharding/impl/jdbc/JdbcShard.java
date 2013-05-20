package net.thumbtack.sharding.impl.jdbc;

import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.Shard;

import java.io.IOException;
import java.util.*;

/**
 * The jdbc shard.
 */
public class JdbcShard implements Shard {

    private static final long serialVersionUID = -6856118381704330390L;

    private static final String SHARD = "shard.";
    private static final String DRIVER = ".driver";
    private static final String URL = ".url";
    private static final String USER = ".user";
    private static final String PASSWORD = ".password";

    private int id;
    private String driver;
    private String url;
    private String user;
    private String password;

    /**
     * Builds list of jdbc shard configurations from properties.
     * @param props The properties.
     * @return The list of jdbc shard configurations.
     */
    public static List<Shard> fromProperties(Properties props) {
        Set<Integer> shardIds = new HashSet<Integer>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(SHARD)) {
                int id = Integer.parseInt(name.split("\\.")[1]);
                shardIds.add(id);
            }
        }
        List<Shard> result = new ArrayList<Shard>(shardIds.size());
        for (int shardId : shardIds) {
            String driver = props.getProperty(SHARD + shardId + DRIVER);
            String url = props.getProperty(SHARD + shardId + URL);
            String user = props.getProperty(SHARD + shardId + USER);
            String password = props.getProperty(SHARD + shardId + PASSWORD);
            result.add(new JdbcShard(shardId, driver, url, user, password));
        }
        return result;
    }

    /**
     * Constructor.
     * @param id The shard id.
     * @param driver The sql driver full class name.
     * @param url The url to database.
     * @param user The database user.
     * @param password The database password.
     */
    public JdbcShard(int id, String driver, String url, String user, String password) {
        this.id = id;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public Connection getConnection() {
        return new JdbcConnection(driver, url, user, password);
    }

    @Override
    public String toString() {
        return "JdbcShard{" +
                "id=" + id +
                ", url='" + url + '\'' +
                ", driver='" + driver + '\'' +
                '}';
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(id);
        out.writeObject(driver);
        out.writeObject(url);
        out.writeObject(user);
        out.writeObject(password);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        id = in.readInt();
        driver = (String) in.readObject();
        url = (String) in.readObject();
        user = (String) in.readObject();
        password = (String) in.readObject();
    }
}
