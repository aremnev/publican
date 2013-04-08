package net.thumbtack.sharding;

import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
abstract public class AbstractDaoTest {

	private static final Logger LOGGER = LoggerFactory.getLogger("AbstractDaoTest");
	private static Server dbServer;

	@BeforeClass
	public static void startEmbeddedDbServer() throws Exception {
		dbServer = TestingUtil.startDbServer();
	}

	@AfterClass
	public static void stopEmbeddedDbServer() {
		TestingUtil.stopDbServer(dbServer);
	}

}
