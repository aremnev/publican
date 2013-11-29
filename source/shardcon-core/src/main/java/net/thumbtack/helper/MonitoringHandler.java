package net.thumbtack.helper;

import org.apache.commons.lang3.ClassUtils;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import static net.thumbtack.helper.Util.loggable;

/**
 * Handler that monitors all invocations and logs all results when execution time exceeds given threshold.
 */
public class MonitoringHandler implements InvocationHandler {

    /**
     * The prefix for Simon stopwatches. Stopwatches names are made by template:
     * prefix + short class name + method name
     */
    public static final String STOPWATCH_PREFIX = "mon.";

    private static final int NANOS_TO_MILLIS_FACTOR = 1000 * 1000;

    private static final Logger logger = LoggerFactory.getLogger("MonitoringHandler");
    private static final String ERROR_TEMPLATE = "Error of execution. Object: {}. Method: {}.{}, with args: {}.";
    private static final String SLOW_TEMPLATE = "Time: {} ms. Object: {}. Method: {}.{}, with args: {}. Result: {}";

    private Object impl;
    private int threshold;

    /**
     * Constructor.
     * @param impl The implementation of interface
     * @param threshold The threshold for logging of slow invocations
     */
    public MonitoringHandler(Object impl, int threshold) {
        this.impl = impl;
        this.threshold = threshold;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object result;

        Stopwatch stopwatch = SimonManager.getStopwatch(
                STOPWATCH_PREFIX + ClassUtils.getShortClassName(impl.getClass()) + "." + method.getName());
        Split split = stopwatch.start();

        try {
            result = method.invoke(impl, args);
        } catch (Throwable t) {
            logger.error(
                    ERROR_TEMPLATE,
                    ClassUtils.getShortClassName(impl.getClass()), method.getName(), loggable(args)
            );
            throw t.getCause();
        }

        long total = split.stop() / NANOS_TO_MILLIS_FACTOR;
        if (total > threshold) {
            logger.warn(
                    SLOW_TEMPLATE,
                    total, ClassUtils.getShortClassName(impl.getClass()), method.getName(), loggable(args), loggable(result)
            );
        }

        return result;
    }
}
