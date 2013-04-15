package net.thumbtack.helper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Random;

public class Util {

    /**
     * Get random element from the list. It is recommended to have
     * random accessible list, e.g. ArrayList, since the method is
     * using get(int)
     */
    public static <T> T getRandom(Random random, List<T> list) {
        return list.get(random.nextInt(list.size()));
    }

    public static String readResource(String resource) throws IOException {
        Reader reader = getResourceAsReader(resource);
        char[] arr = new char[8 * 1024]; // 8K at a time
        StringBuilder buf = new StringBuilder();
        int numChars;
        while ((numChars = reader.read(arr, 0, arr.length)) > 0) {
            buf.append(arr, 0, numChars);
        }
        return buf.toString();
    }

    /**
     * Returns a resource on the classpath as a Reader object
     *
     * @param resource The resource to find
     * @return The resource
     * @throws java.io.IOException If the resource cannot be found or read
     */
    public static Reader getResourceAsReader(String resource) throws IOException {
        return new InputStreamReader(getResourceAsStream(resource));
    }

    /**
     * Returns a resource on the classpath as a Stream object
     *
     * @param resource The resource to find
     * @return The resource
     * @throws java.io.IOException If the resource cannot be found or read
     */
    public static InputStream getResourceAsStream(String resource) throws IOException {
        return getResourceAsStream(resource, (ClassLoader) null);
    }

    /**
     * Returns a resource on the classpath as a Stream object
     *
     * @param resource The resource to find
     * @return The resource
     * @throws java.io.IOException If the resource cannot be found or read
     */
    public static InputStream getResourceAsStream(String resource, ClassLoader classLoader) throws IOException {
        return getResourceAsStream(resource, getClassLoaders(classLoader));
    }

    /**
     * Try to get a resource from a group of classloaders
     *
     * @param resource    - the resource to get
     * @param classLoader - the classloaders to examine
     * @return the resource or null
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

    public static ClassLoader[] getClassLoaders(ClassLoader classLoader) {
        return new ClassLoader[]{
                classLoader,
                Thread.currentThread().getContextClassLoader(),
                Util.class.getClassLoader(),
                ClassLoader.getSystemClassLoader()
        };
    }
}
