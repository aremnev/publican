package net.thumbtack.sharding.core;

import java.util.Collection;
import java.util.List;

public class SelectShard extends Query {

	@Override
	public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
		U result = null;
		for (Connection connection : shards) {
			connection.open();
			try {
				result = closure.call(connection);
			} finally {
				connection.close();
			}
			if (result != null) {
				if (result instanceof Collection<?>) {
					if (((Collection<?>) result).size() > 0) {
						return result;
					}
				} else {
					return result;
				}
			}
		}

		return result;
	}
}
