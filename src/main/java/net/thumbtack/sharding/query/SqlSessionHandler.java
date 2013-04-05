package net.thumbtack.sharding.query;

import org.apache.ibatis.session.SqlSession;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class SqlSessionHandler implements InvocationHandler {

	public static final String GET_MAPPER = "getMapper";
	private SqlSession sqlSession;
	private int shardId;

	private int thresholdInfo;
	private int thresholdWarn;
	private int thresholdError;

	public SqlSessionHandler(SqlSession sqlSession, int shardId) {
		this.sqlSession = sqlSession;
		this.shardId = shardId;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if (GET_MAPPER.equals(method.getName())) {
			Object mapper = method.invoke(sqlSession, args);
			MonitoredMapperHandler handler = new MonitoredMapperHandler(mapper, shardId);
			handler.setSlowThreshold(thresholdInfo, thresholdWarn, thresholdError);
			return Proxy.newProxyInstance(
					getClass().getClassLoader(),
					new Class[]{ (Class) args[0]},
					handler);
		} else {
			return method.invoke(sqlSession, args);
		}
	}

	public void setSlowThreshold(int thresholdInfo, int thresholdWarn, int thresholdError) {
		this.thresholdInfo = thresholdInfo;
		this.thresholdWarn = thresholdWarn;
		this.thresholdError = thresholdError;
	}
}
