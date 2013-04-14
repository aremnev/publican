package net.thumbtack.helper;

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
}
