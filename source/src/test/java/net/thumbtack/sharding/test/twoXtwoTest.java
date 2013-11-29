package net.thumbtack.sharding.test;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class twoXtwoTest extends ShardingTest {

    @Test
    public void twoX2Test() {
        assertEquals(4, 2*2);
    }

    @Override
    protected Logger logger() {
        return LoggerFactory.getLogger(twoXtwoTest.class);
    }
}
