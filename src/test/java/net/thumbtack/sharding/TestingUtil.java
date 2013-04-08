package net.thumbtack.sharding;

import org.apache.ibatis.io.Resources;
import org.h2.tools.Server;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * Here we collect utility methods to use in tests.
 */
@Ignore("This is utility test class, it does NOT contain any tests")
public final class TestingUtil {
	static final Logger LOGGER = LoggerFactory.getLogger("TestUtil");

	private TestingUtil() {}

	public static Server startDbServer() throws SQLException {
		// start H2 server here
		org.h2.Driver.load();
		String[] params = new String[] {"-tcp", "-tcpAllowOthers"};
		Server dbServer = Server.createTcpServer(params);
		dbServer.start();
		LOGGER.debug("Database server started");
		return dbServer;
	}

	public static void stopDbServer(Server dbServer) {
		dbServer.stop();
		LOGGER.debug("Database server stopped");
	}

	public static Properties loadProperties(String proprtiesFile) {
		Properties props = new Properties();
		try {
			props.load(Resources.getResourceAsReader(proprtiesFile));
		} 
		catch (IOException e) {
			LOGGER.error("failed to load properties file: " + proprtiesFile, e);
		}
		return props;
	}

}
