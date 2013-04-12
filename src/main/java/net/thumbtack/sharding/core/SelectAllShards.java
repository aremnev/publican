package net.thumbtack.sharding.core;

import java.util.LinkedList;
import java.util.List;

public class SelectAllShards extends Query {

	@Override
	@SuppressWarnings("unchecked")
	public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
		List<Object> result = new LinkedList<Object>();
		for (Connection connection : shards) {
			connection.open();
			try {
				U elements = closure.call(connection);
				if (elements != null){
					result.addAll((List) elements);
				}
			} finally {
				connection.close();
			}
		}
		return (U) result;
	}
}
