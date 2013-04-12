package net.thumbtack.sharding.core;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

public class MonitoredMapperHandler implements InvocationHandler {

	public static final int NANOS_TO_MILLIS_FACTOR = 1000 * 1000;
	public static final String STOPWATCH_PREFIX = "dao.";

	private static final Logger logger = LoggerFactory.getLogger("MonitoredMapperHandler");
	public static final Logger slowLog = LoggerFactory.getLogger("slow-dao");
	public static final String SLOW_TEMPLATE = "Time: {} ms. Shard: {}. Method: {}.{}, with args: {}. Result: {}";

	private int thresholdInfo = Integer.MAX_VALUE;
	private int thresholdWarn = Integer.MAX_VALUE;
	private int thresholdError = Integer.MAX_VALUE;

	private Object mapper;
	private int shardId;

	public MonitoredMapperHandler(Object mapper, int shardId) {
		this.mapper = mapper;
		this.shardId = shardId;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Object result;

		Stopwatch stopwatch = SimonManager.getStopwatch(
				STOPWATCH_PREFIX + ClassUtils.getShortClassName(method.getDeclaringClass()) + "." + method.getName());
		Split split = stopwatch.start();

		try {
			result = method.invoke(mapper, args);
		} catch (Throwable t) {
			logger.error(
					"Error of execution query to db. Shard: {}. Method: {}.{}, with args: {}.",
					new Object[]{shardId, mapperName(), method.getName(), loggable(args)}
			);
			throw t.getCause();
		}

		long total = split.stop() / NANOS_TO_MILLIS_FACTOR;
		if (total > thresholdError) {
			slowLog.error(SLOW_TEMPLATE, new Object[]{total, shardId, mapperName(), method.getName(), loggable(args), loggable(result)});
		} else if (total > thresholdWarn) {
			slowLog.warn(SLOW_TEMPLATE, new Object[]{total, shardId, mapperName(), method.getName(), loggable(args), loggable(result)});
		} else if (total > thresholdInfo) {
			slowLog.info(SLOW_TEMPLATE, new Object[]{total, shardId, mapperName(), method.getName(), loggable(args), loggable(result)});
		}

		return result;
	}

	public void setSlowThreshold(int thresholdInfo, int thresholdWarn, int thresholdError) {
		if (thresholdInfo > 0) {
			this.thresholdInfo = thresholdInfo;
		}
		if (thresholdWarn > 0) {
			this.thresholdWarn = thresholdWarn;
		}
		if (thresholdError > 0) {
			this.thresholdError = thresholdError;
		}
	}

	private String mapperName() {
		String name = mapper.getClass().getInterfaces()[0].getName();
		return StringUtils.substringAfterLast(name, ".");
	}

	public static Object[] loggable(Object[] objects) {
		if (objects != null) {
			Object[] result = new Object[objects.length];
			for (int i = 0; i < objects.length; i++) {
				result[i] = loggable(objects[i]);
			}
			return result;
		}

		return null;
	}

	@SuppressWarnings("LoopStatementThatDoesntLoop")
	public static Object loggable(Object obj) {
		if (obj == null) {
			return obj;
		}

		if (obj instanceof Collection) {
			Collection c = (Collection) obj;
			for (Object o : c) {
				return "[" + o + " and " + (c.size() - 1) + " other objects]";
			}
		}

		if (obj instanceof Map) {
			Collection keys = ((Map) obj).keySet();
			for (Object key : keys) {
				return "[" + key + "->" + ((Map) obj).get(key) + " and " + (keys.size() - 1) + " other objects]";
			}
		}

		String s = obj.toString();
		return s.length() > 255 ? s.substring(0, 255) + "..." : s;
	}

}
