package de.cofinpro.liquibase;

import org.h2.jdbcx.JdbcConnectionPool;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author Gregor Tudan, Cofinpro AG
 */
@ApplicationScoped
public class H2LiquibaseConfig extends CDILiquibaseConfig {

    private static final String[] TENANTS = {"tenant1", "tenant2", "tenant3", "tenant4", "tenant5"};
    private JdbcConnectionPool dataSource;

    @PostConstruct
    public void prepareDatasource() {
        JdbcConnectionPool cp = JdbcConnectionPool.create(
                "jdbc:h2:mem:test", "sa", "sa");

        try (Connection conn = cp.getConnection();
             Statement statement = conn.createStatement()) {
            for (String tenant : TENANTS) {
                statement.execute("CREATE SCHEMA IF NOT EXISTS " + tenant);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create schemas", e);
        }

        this.dataSource = cp;
    }

    @PreDestroy
    public void destroyDatasource() {
        dataSource.dispose();
    }

    @Override
    public String getChangeLog() {
        return "changeset.yaml";
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Collection<String> getSchemas() {
        return Arrays.asList(TENANTS);
    }
}
