package net.thumbtack.sharding.test;

import org.junit.Rule;
import org.junit.rules.MethodRule;
import org.junit.rules.TestWatchman;
import org.junit.runners.model.FrameworkMethod;
import org.slf4j.Logger;

public abstract class ShardingTest {

    @Rule
    public MethodRule watchman = new TestWatchman() {
        public void starting(FrameworkMethod method) {
            logger().info("{} being run...", method.getName());
        }
    };

    protected abstract Logger logger();
}
