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

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.sql.DataSource;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * CDI integration for liquibase with support for schema-based multi-tenancy.
 * <p>
 * This class runs liquibase after context initialization has finished. Minimum CDI version required is CDI 1.1.
 * <p>
 * This expects an implementation of {@link CDILiquibaseConfig} in the classpath, or CDI initialization will fail.
 * @author Gregor Tudan, Cofinpro AG
 */
public class CDILiquibaseMultiTenant {

    private final Logger log = LogFactory.getInstance().getLog();
    private final Instance<CDILiquibaseConfig> configs;

    @Inject
    public CDILiquibaseMultiTenant(@Any Instance<CDILiquibaseConfig> configs) {
        this.configs = configs;
        log.info("Booting Liquibase " + LiquibaseUtil.getBuildVersion());
    }

    @SuppressWarnings("unused")
    protected void performUpdate(@Observes @Initialized(ApplicationScoped.class) Object ignored) throws LiquibaseException {
       performUpdate();
    }

    void performUpdate() throws LiquibaseException {
        boolean shouldRun = checkIfLiquibaseShouldRun();
        if (shouldRun) {
            for (CDILiquibaseConfig config : configs) {
                try {
                    Liquibase liquibase = createLiquibase(config);
                    if (config.getSchemas() == null || config.getSchemas().isEmpty()) {
                        performUpdate(liquibase, config, config.getDefaultSchema());
                    } else {
                        for (String schema : config.getSchemas()) {
                            performUpdate(liquibase, config, schema);
                        }
                    }
                } catch (LiquibaseException | SQLException e) {
                    throw new UnexpectedLiquibaseException(e);
                }
            }
        }
    }

    private boolean checkIfLiquibaseShouldRun() {
        String hostName = "unknown";
        try {
            hostName = NetUtil.getLocalHostName();
        } catch (UnknownHostException | SocketException e) {
            log.warning("Cannot find hostname: " + e.getMessage(), e);
        }

        LiquibaseConfiguration liquibaseConfiguration = LiquibaseConfiguration.getInstance();
        boolean shouldRun = liquibaseConfiguration.getConfiguration(GlobalConfiguration.class).getShouldRun();
        if (!shouldRun) {
            log.info("Liquibase did not run on " + hostName + " because " + liquibaseConfiguration.describeValueLookupLogic(GlobalConfiguration.class, GlobalConfiguration.SHOULD_RUN) + " was set to false");
        }
        return shouldRun;
    }


    private void performUpdate(Liquibase liquibase, CDILiquibaseConfig config, String schema) throws LiquibaseException {
        Connection conn = null;
        try {
            conn = getConnectionForSchema(config, schema);
            liquibase.getDatabase().setConnection(new JdbcConnection(conn));
            liquibase.getDatabase().setDefaultSchemaName(schema);
            if (config.isDropFirst()) {
                liquibase.dropAll();
            }
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

    private Connection getConnectionForSchema(CDILiquibaseConfig config, String schema) throws SQLException {
        if (config.getSchemaCredentials() != null) {
            String[] credentials = config.getSchemaCredentials().get(schema);
            if (credentials != null && credentials.length == 2) {
                return config.getDataSource().getConnection(credentials[0], credentials[1]);
            }
        }
        return config.getDataSource().getConnection();
    }

    private Liquibase createLiquibase(CDILiquibaseConfig config) throws LiquibaseException, SQLException {
        final Liquibase liquibase;
        DataSource c = config.getDataSource();
        try (Connection conn = c.getConnection()) {
            liquibase = new Liquibase(config.getChangeLog(), config.getResourceAccessor(), createDatabase(config, conn));
        }

        if (config.getParameters() != null) {
            for (Map.Entry<String, String> entry : config.getParameters().entrySet()) {
                liquibase.setChangeLogParameter(entry.getKey(), entry.getValue());
            }
        }
        return liquibase;
    }


    private Database createDatabase(CDILiquibaseConfig config, Connection c) throws DatabaseException {
        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(c));
        if (config.getDefaultSchema() != null) {
            database.setDefaultSchemaName(config.getDefaultSchema());
        }
        return database;
    }
}
