package net.thumbtack.shardcon.chunk;

import net.thumbtack.shardcon.core.query.QueryClosure;

public interface RemoveHelper {

    /**
     * @param idFrom Min id.
     * @param idTo Max id.
     * @return Function that removes entities with id in [idFrom, idTo]
     */
    QueryClosure<Void> remove(long idFrom, long idTo);
}
