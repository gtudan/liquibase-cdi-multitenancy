package de.cofinpro.liquibase;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.configuration.GlobalConfiguration;
import liquibase.configuration.LiquibaseConfiguration;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.util.LiquibaseUtil;
import liquibase.util.NetUtil;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.sql.DataSource;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Gregor Tudan, Cofinpro AG
 */
public class CDILiquibaseMultiTenant {

    private final Logger log = LogFactory.getInstance().getLog();
    private Liquibase liquibase;
    private boolean shouldRun;

    @Inject
    private CDILiquibaseConfig config;

    @PostConstruct
    public void onStartup() {
        log.info("Booting Liquibase " + LiquibaseUtil.getBuildVersion());
        checkIfLiquibaseShouldRun();
        if (this.shouldRun) {
            try {
                createLiquibase(config.getDataSource());
            } catch (LiquibaseException | SQLException e) {
                throw new UnexpectedLiquibaseException(e);
            }
        }
    }

    public void performUpdate(@Observes @Initialized(ApplicationScoped.class) Object ignored) throws LiquibaseException {
        if (!shouldRun) {
            return;
        }

        if (config.getSchemas().isEmpty()) {
            performUpdate(liquibase, config.getDefaultSchema());
        } else {
            for (String schema : config.getSchemas()) {
                performUpdate(liquibase, schema);
            }
        }
    }

    private void checkIfLiquibaseShouldRun() {
        String hostName = "unknown";
        try {
            hostName = NetUtil.getLocalHostName();
        } catch (UnknownHostException | SocketException e) {
            log.warning("Cannot find hostname: " + e.getMessage(), e);
        }

        LiquibaseConfiguration liquibaseConfiguration = LiquibaseConfiguration.getInstance();
        this.shouldRun = liquibaseConfiguration.getConfiguration(GlobalConfiguration.class).getShouldRun();
        if (!this.shouldRun) {
            log.info("Liquibase did not run on " + hostName + " because " + liquibaseConfiguration.describeValueLookupLogic(GlobalConfiguration.class, GlobalConfiguration.SHOULD_RUN) + " was set to false");
        }
    }


    private void performUpdate(Liquibase liquibase, String schema) throws LiquibaseException {
        Connection conn = null;
        try {
            final String[] credentials = config.getSchemaCredentials().get(schema);
            if (credentials != null && credentials.length == 2) {
                conn = config.getDataSource().getConnection(credentials[0], credentials[1]);
            } else {
                conn = config.getDataSource().getConnection();
            }
            liquibase.getDatabase().setConnection(new JdbcConnection(conn));
            liquibase.getDatabase().setDefaultSchemaName(schema);
            liquibase.update(new Contexts(config.getContexts()), new LabelExpression(config.getLabels()));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            if (liquibase != null && liquibase.getDatabase() != null) {
                liquibase.getDatabase().close();
            } else if (conn != null) {
                try {
                    conn.rollback();
                    conn.close();
                } catch (SQLException e) {
                    //nothing to do
                }
            }
        }
    }

    private void createLiquibase(DataSource c) throws LiquibaseException, SQLException {
        try (Connection conn = c.getConnection()) {
            this.liquibase = new Liquibase(config.getChangeLog(), config.getResourceAccessor(), createDatabase(conn));
        }

        if (config.getParameters() != null) {
            for (Map.Entry<String, String> entry : config.getParameters().entrySet()) {
                this.liquibase.setChangeLogParameter(entry.getKey(), entry.getValue());
            }
        }
    }


    private Database createDatabase(Connection c) throws DatabaseException {
        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(c));
        if (config.getDefaultSchema() != null) {
            database.setDefaultSchemaName(config.getDefaultSchema());
        }
        return database;
    }
}
