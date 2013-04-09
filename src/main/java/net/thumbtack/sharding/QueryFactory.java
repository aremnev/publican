package net.thumbtack.sharding;

import net.thumbtack.helper.NamedThreadFactory;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static net.thumbtack.sharding.QueryType.*;

public class QueryFactory implements QueryEngine {

	public Random random = new Random();

	private Map<QueryType, Query> queries;

	private Map<QueryType, Query> queriesParallel;

	private boolean parallelMode = true;

	private boolean monitoringEnabled = false;

	private int thresholdInfo;
	private int thresholdWarn;
	private int thresholdError;

	private Map<Integer, SqlSessionFactory> sessionFactoryMap;

	private ExecutorService queryExecutor;

	public QueryFactory(Map<Integer, SqlSessionFactory> sessionFactoryMap) {
		this.sessionFactoryMap = sessionFactoryMap;

		queries = new HashMap<QueryType, Query>();
		queries.put(QueryType.selectSpecShard, new SelectSpecShard(this));
		queries.put(QueryType.selectShard, new SelectShard(this));
		queries.put(QueryType.selectAnyShard, new SelectAnyShard(this));
		queries.put(QueryType.selectAllShards, new SelectAllShards(this));
		queries.put(QueryType.selectAllShardsSum, new SelectAllShardsSum(this));
		queries.put(QueryType.updateSpecShard, new UpdateSpecShard(this));
		queries.put(QueryType.updateAllShards, new UpdateAllShards(this));

		queriesParallel = new HashMap<QueryType, Query>();
		queriesParallel.put(QueryType.selectSpecShard, new SelectSpecShard(this));
		queriesParallel.put(QueryType.selectShard, new SelectShardParallel(this));
		queriesParallel.put(QueryType.selectAnyShard, new SelectAnyShard(this));
		queriesParallel.put(QueryType.selectAllShards, new SelectAllShardsParallel(this));
		queriesParallel.put(QueryType.selectAllShardsSum, new SelectAllShardsSumParallel(this));
		queriesParallel.put(QueryType.updateSpecShard, new UpdateSpecShard(this));
		queriesParallel.put(QueryType.updateAllShards, new UpdateAllShardsParallel(this));
	}

	public void cleanup() {
		if (queryExecutor != null) {
			queryExecutor.shutdown();
		}
	}

	public void setParallelMode(boolean mode) {
		parallelMode = mode;
	}

	public void setMonitoringEnabled(boolean enable) {
		monitoringEnabled = enable;
	}

	public void setSlowThreshold(int thresholdInfo, int thresholdWarn, int thresholdError) {
		this.thresholdInfo = thresholdInfo;
		this.thresholdWarn = thresholdWarn;
		this.thresholdError = thresholdError;
	}

	public Query getQuery(QueryType type) {
		return parallelMode ? queriesParallel.get(type) : queries.get(type);
	}

	@Override
	public Collection<Integer> shardsKeys() {
		return sessionFactoryMap.keySet();
	}

	public SqlSession openSession(long id) {
		return wrapSession(sessionFactory(id).openSession(), shardIndex(id));
	}

	@Override
	public SqlSession openSession(ExecutorType type) {
		return openSession(randomShardNum(), type);
	}

	@Override
	public SqlSession openSession(long id, ExecutorType type) {
		return wrapSession(sessionFactory(id).openSession(type), shardIndex(id));
	}

	@Override
	public SqlSessionFactory sessionFactory(long id) {
		return sessionFactoryMap.get(shardIndex(id));
	}

	@Override
	public int shardsCount() {
		return sessionFactoryMap.size();
	}

	@Override
	public void execute(Runnable command) {
		queryExecutor.execute(command);
	}

	public void setThreadPoolSize(int size) {
		if (queryExecutor != null) {
			queryExecutor.shutdown();
		}
		queryExecutor = Executors.newFixedThreadPool(size, new NamedThreadFactory("query"));
	}

	public int shardIndex(long id) {
		return (int) (id % shardsCount());
	}

	private int randomShardNum() {
		return shardIndex(shardsCount() == 1 ? 0 : Math.abs(random.nextLong()));
	}

	private SqlSession wrapSession(SqlSession sqlSession, int shardId) {
		if (monitoringEnabled) {
			SqlSessionHandler handler = new SqlSessionHandler(sqlSession, shardId);
			handler.setSlowThreshold(thresholdInfo, thresholdWarn, thresholdError);
			return (SqlSession) Proxy.newProxyInstance(
					getClass().getClassLoader(),
					new Class[]{SqlSession.class},
					handler);
		} else {
			return sqlSession;
		}
	}
}
