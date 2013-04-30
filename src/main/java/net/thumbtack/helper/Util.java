package net.thumbtack.helper;

import fj.F;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Field;
import java.util.*;

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
     * Get random element from the array.
     * @param random The random to use.
     * @param array The array to choose from.
     * @param <T> The type of elements.
     * @return The chosen element.
     */
    public static <T> T getRandom(Random random, T[] array) {
        return array[random.nextInt(array.length)];
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
                if (null == returnValue) {
                    returnValue = cl.getResourceAsStream("/" + resource);
                }

                if (null != returnValue) {
                    return returnValue;
                }
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

    /**
     * Maps the list to the map with the given mapping function.
     * @param list The list to map.
     * @param mapping The mapping function.
     * @param <K> The key type.
     * @param <V> The value type.
     * @return The map.
     */
    public static <K, V> Map<K, V> index(List<V> list, F<V, K> mapping) {
        Map<K, V> map = new HashMap<K, V>(list.size());
        for (V v : list) {
            map.put(mapping.f(v), v);
        }
        return map;
    }

    /**
     * Builds a new list by applying a function to all elements of this list.
     * @param list The list.
     * @param mapping The function to apply to each element.
     * @param <A> The element type of the returned collection.
     * @param <B> The element type of the list.
     * @return A new list resulting from applying the given function f to each element of this list and collecting the results.
     */
    public static <A, B> List<B> map(List<A> list, F<A, B> mapping) {
        List<B> res = new ArrayList<B>(list.size());
        for (A a : list) {
            res.add(mapping.f(a));
        }
        return res;
    }

    /**
     * Return a list of all fields (whatever access status, and on whatever
     * superclass they were defined) that can be found on this class.
     * This is like a union of {@link Class#getDeclaredFields()} which
     * ignores and super-classes, and {@link Class#getFields()} which ignored
     * non-public fields
     * @param clazz The class to introspect
     * @return The complete list of fields
     */
    public static Field[] getAllFields(Class<?> clazz) {
        List<Class<?>> classes = getAllSuperclasses(clazz);
        classes.add(clazz);
        return getAllFields(classes);
    }
    /**
     * As {@link #getAllFields(Class)} but acts on a list of {@link Class}s and
     * uses only {@link Class#getDeclaredFields()}.
     * @param classes The list of classes to reflect on
     * @return The complete list of fields
     */
    private static Field[] getAllFields(List<Class<?>> classes) {
        Set<Field> fields = new HashSet<Field>();
        for (Class<?> clazz : classes) {
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
        }

        return fields.toArray(new Field[fields.size()]);
    }
    /**
     * Return a List of super-classes for the given class.
     * @param clazz the class to look up
     * @return the List of super-classes in order going up from this one
     */
    public static List<Class<?>> getAllSuperclasses(Class<?> clazz) {
        List<Class<?>> classes = new ArrayList<Class<?>>();

        Class<?> superclass = clazz.getSuperclass();
        while (superclass != null) {
            classes.add(superclass);
            superclass = superclass.getSuperclass();
        }

        return classes;
    }
}
