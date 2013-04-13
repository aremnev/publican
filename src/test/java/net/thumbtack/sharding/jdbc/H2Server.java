package net.thumbtack.sharding.jdbc;

import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class H2Server {

    private static final Logger logger = LoggerFactory.getLogger(H2Server.class);

    private Server server;

    public Server start() throws SQLException {
        if (server != null)
            throw new RuntimeException("H2 server already started");
        // start H2 server here
        org.h2.Driver.load();
        String[] params = new String[] {"-tcp", "-tcpAllowOthers"};
        Server dbServer = Server.createTcpServer(params);
        dbServer.start();
        logger.debug("Database server started");
        return dbServer;
    }

    public void stop() {
        if (server != null)
            server.stop();
        logger.debug("Database server stopped");
    }
}
