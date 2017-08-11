package de.cofinpro.liquibase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Gregor Tudan, Cofinpro AG
 */
public class CDITest {

    private SeContainer container;

    private H2LiquibaseConfig h2LiquibaseConfig;

    @Before
    public void setUp() throws Exception {
        container = SeContainerInitializer.newInstance()
                .addBeanClasses(CDILiquibaseMultiTenant.class, H2LiquibaseConfig.class)
                .initialize();
        this.h2LiquibaseConfig = container.select(H2LiquibaseConfig.class).get();
    }

    @After
    public void tearDown() throws Exception {
        container.close();
    }

    @Test
    public void testWeldStartup() throws Exception {
        assertThat(h2LiquibaseConfig, is(notNullValue()));

        final String query = "SELECT * FROM tenant2.user";
        try (Connection conn = h2LiquibaseConfig.getDataSource().getConnection();
             Statement statement = conn.createStatement();
             ResultSet result = statement.executeQuery(query);
        ) {
            assertThat(result.first(), is(true));
            assertThat(result.getString("name"), is("Bert"));
        }
    }
}
