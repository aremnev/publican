package net.thumbtack.shardcon.chunk;

import net.thumbtack.shardcon.core.Shard;
import net.thumbtack.shardcon.core.query.Connection;
import net.thumbtack.shardcon.core.query.Query;
import net.thumbtack.shardcon.core.query.UpdateSpecShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class Remove implements Callable<Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(Remove.class);

    private Chunk chunk;
    private Shard fromShard;
    private final RemoveHelper helper;

    public Remove(Chunk chunk, Shard fromShard, RemoveHelper helper) {
        this.chunk = chunk;
        this.fromShard = fromShard;
        this.helper = helper;
    }

    @Override
    public Boolean call() throws Exception {
        logger.info("Removing {} from {}", chunk, fromShard);
        try {
            Query update = new UpdateSpecShard();
            update.query(helper.remove(chunk.fromId, chunk.toId), getShardConnection(fromShard));
            return true;
        } catch (Exception e) {
            logger.error("Failed to remove {} from {}", chunk, fromShard);
            return false;
        }
    }

    private List<Connection> getShardConnection(Shard shard) {
        List<Connection> connections = new ArrayList<Connection>(1);
        connections.add(shard.getConnection());
        return connections;
    }
}
