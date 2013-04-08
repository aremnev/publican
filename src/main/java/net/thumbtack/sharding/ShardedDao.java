package net.thumbtack.sharding;


import net.thumbtack.sharding.query.QueryClosure;
import net.thumbtack.sharding.query.SqlQueryEngine;
import net.thumbtack.sharding.query.QueryType;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.*;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Base class for all sharded DAO classes.
 */
abstract public class ShardedDao<T> implements Dao {

	private static final String MYBATIS_CONFIG_XML = "MyBatisConfig.xml";
	private static final String SHARD_PARAM = "shard";

	private ExecutorService selectExecutor;

	protected SqlQueryEngine sqlQueryEngine;

	public static Map<Integer, SqlSessionFactory> buildSessionFactoryMap(Properties props) throws IOException {
		List<Properties> shardProps = extractShardProps(props);
		Map<Integer, SqlSessionFactory> sessionFactoryMap = new HashMap<Integer, SqlSessionFactory>();
		int shardNum = 0;
		for (Properties properties : shardProps) {
			if (! sessionFactoryMap.containsKey(shardNum)) {
				Reader reader = Resources.getResourceAsReader(MYBATIS_CONFIG_XML);
				SqlSessionFactoryBuilder builder = new SqlSessionFactoryBuilder();
				SqlSessionFactory sessionFactory = builder.build(reader, properties);
				initConfiguration(sessionFactory.getConfiguration());
				sessionFactoryMap.put(shardNum, sessionFactory);
			}
			shardNum++;
		}
		return sessionFactoryMap;
	}

	public ShardedDao(SqlQueryEngine sqlQueryEngine) {
		this.sqlQueryEngine = sqlQueryEngine;
		selectExecutor = Executors.newFixedThreadPool(sqlQueryEngine.shardsCount() * 10, new NamedThreadFactory("selectExecutor"));
	}

	@Override
	public void invalidateCache() {
		// do nothing
	}

	@Override
	public void setCacheDisabled(boolean disabled) {
		// do nothing
	}

	@Override
	public boolean getCacheDisabled() {
		return true;
	}

	public static String wrapForLike(String s) {
		StringBuilder sb = new StringBuilder();
		sb.append("%");
		sb.append(s.replace("%", "\\%"));
		sb.append("%");
		return sb.toString();
	}

	public static String leftWrapForLike(String s) {
		StringBuilder sb = new StringBuilder();
		sb.append(s.replace("%", "\\%"));
		sb.append("%");
		return sb.toString();
	}

	public static RowBounds getRowBounds(int page, int size) {
		return new RowBounds(size * (page - 1), size);
	}

	// select from specific shard defined by userId
	protected <U> U selectSpecShard(QueryClosure<U> closure, long id) {
		return sqlQueryEngine.getQuery(QueryType.selectSpecShard).query(closure, id);
	}

	// select from undefined shard
	protected <U> U selectShard(QueryClosure<U> closure) {
		return sqlQueryEngine.getQuery(QueryType.selectShard).query(closure);
	}

	// select from any shard
	protected <U> U selectAnyShard(QueryClosure<U> closure) {
		return sqlQueryEngine.getQuery(QueryType.selectAnyShard).query(closure);
	}

	// select from all shards with results union
	protected <U> List<U> selectAllShards(QueryClosure<List<U>> closure) {
		return sqlQueryEngine.getQuery(QueryType.selectAllShards).query(closure);
	}

	// select from all shards with results union
	protected Integer selectSumAllShards(QueryClosure<Integer> closure) {
		return sqlQueryEngine.getQuery(QueryType.selectAllShardsSum).query(closure);
	}

	// update on specific shard defined by userId
	protected <U> U updateSpecShard(QueryClosure<U> closure, long id) {
		return sqlQueryEngine.getQuery(QueryType.updateSpecShard).query(closure, id);
	}

	// update on all shards
	// the result of the method is the last successful closure call result
	protected <U> U updateAllShards(QueryClosure<U> closure) {
		return sqlQueryEngine.getQuery(QueryType.updateAllShards).query(closure);
	}

	protected <U> Future<U> submit(Callable<U> task) {
		return selectExecutor.submit(task);
	}

	protected static interface IdAccessor<T> {
		long id(T obj);
	}

	private class LongIdAccessor implements IdAccessor<Long> {
		@Override
		public long id(Long obj) {
			return obj;
		}
	}

	private class ReflectionIdAccessor implements IdAccessor<Object> {
		private String methodId;

		ReflectionIdAccessor(String methodId) {
			this.methodId = methodId;
		}

		@Override
		public long id(Object obj) {
			try {
				Method method = obj.getClass().getMethod(methodId);
				return (Long) method.invoke(obj);
			} catch (NullPointerException npe) {
				throw npe;
			} catch (Exception e) {
				throw new IllegalArgumentException("No method " + methodId + " found in class " + obj.getClass().getName());
			}
		}
	}

	protected Map<Integer, List<Long>> splitByShards(List<Long> entities){
		return splitByShards(entities, new LongIdAccessor());
	}

	protected <T> Map<Integer, List<T>> splitByShards(List<T> entities, IdAccessor<T> accessor) {
		Map<Integer, List<T>> result = new HashMap<Integer, List<T>>();
		for (T entity : entities) {
		    Long id = accessor.id(entity);
			int shardNum = sqlQueryEngine.shardIndex(id);
			if (!result.containsKey(shardNum)) {
				result.put(shardNum, new ArrayList<T>());
			}
			result.get(shardNum).add(entity);
		}
		return result;
	}

	protected <T> Map<Integer, List<T>> splitByShards(List<T> entities, String idMethod){
		IdAccessor accessor = (StringUtils.isEmpty(idMethod)) ? new LongIdAccessor() : new ReflectionIdAccessor(idMethod);
		return splitByShards(entities, accessor);
	}

	private static List<Properties> extractShardProps(Properties props) {
		Map<Integer, Properties> shards = new HashMap<Integer, Properties>();
		for (String propertyName : props.stringPropertyNames()) {
			if (propertyName.toLowerCase().indexOf(SHARD_PARAM) == 0) {
				int shardNum = Integer.parseInt(propertyName.split("\\.")[1]);
				if (! shards.containsKey(shardNum)) {
					shards.put(shardNum, new Properties());
				}
				shards.get(shardNum).put(propertyName.replace(SHARD_PARAM + "." + shardNum + ".", ""), props.get(propertyName));
			}
		}
		return new ArrayList<Properties>(shards.values());
	}

	private static void initConfiguration(Configuration configuration) {
//		configuration.addMapper(LocationMapper.class);
//		configuration.getTypeHandlerRegistry().register(JdbcType.TIMESTAMP, new DateTypeHandler());
	}

	protected static String getCatalogFromSession(SqlSession session) {
		try {
			return session.getConnection().getCatalog();
		} catch (SQLException e) {
			return "";
		}
	}
}
