package net.thumbtack.helper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Util class.
 */
public class Util {

    private static final int RESOURCE_READING_PORTION = 8 * 1024;

    private static final int MAX_LOGGABLE_LENGTH = 255;

    /**
     * Get random element from the list. It is recommended to have
     * random accessible list, e.g. ArrayList, since the method is
     * using get(int).
     * @param random The random to use.
     * @param list The list to choose from.
     * @param <T> The type of elements.
     * @return The chosen element.
     */
    public static <T> T getRandom(Random random, List<T> list) {
        return list.get(random.nextInt(list.size()));
    }

    /**
     * Finds and reads resource into a string.
     *
     * @param resource The resource to find.
     * @return The content of resource.
     * @throws IOException If the resource cannot be found or read.
     */
    public static String readResource(String resource) throws IOException {
        Reader reader = getResourceAsReader(resource);
        char[] arr = new char[RESOURCE_READING_PORTION]; // 8K at a time
        StringBuilder buf = new StringBuilder();
        int numChars;
        while ((numChars = reader.read(arr, 0, arr.length)) > 0) {
            buf.append(arr, 0, numChars);
        }
        return buf.toString();
    }

    /**
     * Returns a resource on the classpath as a Reader object.
     *
     * @param resource The resource to find.
     * @return The resource.
     * @throws java.io.IOException If the resource cannot be found or read.
     */
    public static Reader getResourceAsReader(String resource) throws IOException {
        return new InputStreamReader(getResourceAsStream(resource));
    }

    /**
     * Returns a resource on the classpath as a Stream object.
     *
     * @param resource The resource to find.
     * @return The resource.
     * @throws java.io.IOException If the resource cannot be found or read.
     */
    public static InputStream getResourceAsStream(String resource) throws IOException {
        return getResourceAsStream(resource, (ClassLoader) null);
    }

    /**
     * Returns a resource on the classpath as a Stream object.
     * @param resource The resource to find.
     * @param classLoader The classloader to examine.
     * @return The resource.
     * @throws IOException If the resource cannot be found or read.
     */
    public static InputStream getResourceAsStream(String resource, ClassLoader classLoader) throws IOException {
        return getResourceAsStream(resource, getClassLoaders(classLoader));
    }

    /**
     * Try to get a resource from a group of classloaders.
     *
     * @param resource The resource to get.
     * @param classLoader The classloaders to examine.
     * @return The resource or null.
     */
    public static InputStream getResourceAsStream(String resource, ClassLoader[] classLoader) {
        for (ClassLoader cl : classLoader) {
            if (null != cl) {

                // try to find the resource as passed
                InputStream returnValue = cl.getResourceAsStream(resource);

                // now, some class loaders want this leading "/", so we'll add it and try again if we didn't find the resource
                if (null == returnValue) returnValue = cl.getResourceAsStream("/" + resource);

                if (null != returnValue) return returnValue;
            }
        }
        return null;
    }

    /**
     * Gets all class loaders that we can get in the context.
     * @param classLoader The class loader to top of list.
     * @return The Array of class loaders.
     */
    public static ClassLoader[] getClassLoaders(ClassLoader classLoader) {
        return new ClassLoader[]{
                classLoader,
                Thread.currentThread().getContextClassLoader(),
                Util.class.getClassLoader(),
                ClassLoader.getSystemClassLoader()
        };
    }

    /**
     * Makes loggable objects.
     * @param objects The object.
     * @return The loggable objects.
     */
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

    /**
     * Makes loggable object.
     * @param obj The object.
     * @return The loggable object.
     */
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
        return s.length() > MAX_LOGGABLE_LENGTH ? s.substring(0, MAX_LOGGABLE_LENGTH) + "..." : s;
    }
}
