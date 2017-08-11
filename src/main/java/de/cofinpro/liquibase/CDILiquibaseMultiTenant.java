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
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.util.LiquibaseUtil;
import liquibase.util.NetUtil;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Gregor Tudan, Cofinpro AG
 */
@ApplicationScoped
public class CDILiquibaseMultiTenant {

    private Logger log = LogFactory.getInstance().getLog();

    @Inject
    private de.cofinpro.liquibase.CDILiquibaseConfig config;

    @PostConstruct
    public void onStartup() {
        log.info("Booting Liquibase " + LiquibaseUtil.getBuildVersion());
        String hostName;
        try {
            hostName = NetUtil.getLocalHostName();
        } catch (Exception e) {
            log.warning("Cannot find hostname: " + e.getMessage());
            log.debug("", e);
            return;
        }

        LiquibaseConfiguration liquibaseConfiguration = LiquibaseConfiguration.getInstance();
        if (!liquibaseConfiguration.getConfiguration(GlobalConfiguration.class).getShouldRun()) {
            log.info("Liquibase did not run on " + hostName + " because " + liquibaseConfiguration.describeValueLookupLogic(GlobalConfiguration.class, GlobalConfiguration.SHOULD_RUN) + " was set to false");
        }
    }

    public void performUpdate(@Observes @Initialized(ApplicationScoped.class) Object ignored) throws LiquibaseException {
        final Liquibase liquibase;
        try (Connection conn = config.getDataSource().getConnection()) {
            liquibase = createLiquibase(conn);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        if (config.getSchemas().isEmpty()) {
            performUpdate(liquibase, config.getDefaultSchema());
        } else {
            for (String tenant : config.getSchemas()) {
                performUpdate(liquibase, tenant);
            }
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
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.rollback();
                    conn.close();
                }
            } catch (SQLException e) {
                //nothing to do
            }
        }
    }

    private Liquibase createLiquibase(Connection c) throws LiquibaseException {
        Liquibase liquibase = new Liquibase(config.getChangeLog(), config.getResourceAccessor(), createDatabase(c));
        if (config.getParameters() != null) {
            for (Map.Entry<String, String> entry : config.getParameters().entrySet()) {
                liquibase.setChangeLogParameter(entry.getKey(), entry.getValue());
            }
        }

        return liquibase;
    }

    private Database createDatabase(Connection c) throws DatabaseException {
        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(c));
        if (config.getDefaultSchema() != null) {
            database.setDefaultSchemaName(config.getDefaultSchema());
        }
        return database;
    }
}
