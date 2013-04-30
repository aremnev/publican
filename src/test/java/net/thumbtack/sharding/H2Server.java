package net.thumbtack.sharding;

import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class H2Server { // TODO delete?

    private static final Logger logger = LoggerFactory.getLogger(H2Server.class);

    private Server server;

    public H2Server() throws SQLException {
        if (server != null)
            throw new RuntimeException("H2 server already started");
        // start H2 server here
        org.h2.Driver.load();
        String[] params = new String[] {"-tcp", "-tcpAllowOthers"};
        server = Server.createTcpServer(params);
    }

    public Server start() throws SQLException {
        server.start();
        logger.debug("Database server started");
        return server;
    }

    public void stop() {
        server.stop();
        logger.debug("Database server stopped");
    }
}
