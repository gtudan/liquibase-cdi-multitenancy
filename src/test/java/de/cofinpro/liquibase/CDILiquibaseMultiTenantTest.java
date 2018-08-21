package de.cofinpro.liquibase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.enterprise.inject.Instance;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Gregor Tudan, Cofinpro AG
 */
@RunWith(MockitoJUnitRunner.class)
public class CDILiquibaseMultiTenantTest {

    @Mock
    private Instance<CDILiquibaseConfig> configs;

    @Mock
    private Iterator<CDILiquibaseConfig> it;

    private H2LiquibaseConfig config;
    
    @Before
    public void setUp() {
        config = new H2LiquibaseConfig();
        Mockito.when(configs.iterator()).thenReturn(it);
        Mockito.when(it.hasNext()).thenReturn(true).thenReturn(false);
        Mockito.when(it.next()).thenReturn(config);
    }

    @After
    public void tearDown() {
        config.close();
    }

    @Test
    public void performUpdate() throws Exception {
        CDILiquibaseMultiTenant liquibase = new CDILiquibaseMultiTenant(configs);
        liquibase.performUpdate();

        final String query = "SELECT * FROM tenant2.user";
        try (Connection conn = config.getDataSource().getConnection();
             Statement statement = conn.createStatement();
             ResultSet result = statement.executeQuery(query);
        ) {
            assertThat(result.first(), is(true));
            assertThat(result.getString("name"), is("Bert"));
        }

    }

}