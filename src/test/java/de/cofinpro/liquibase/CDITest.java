package de.cofinpro.liquibase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Integration test that checks if liquibase really starts during CDI initialization
 *
 * @author Gregor Tudan, Cofinpro AG
 */
public class CDITest {

    private SeContainer container;

    @Before
    public void setUp() throws Exception {
        container = SeContainerInitializer.newInstance()
                .addBeanClasses(CDILiquibaseMultiTenant.class, H2LiquibaseConfig.class)
                .initialize();
    }

    @After
    public void tearDown() throws Exception {
        container.close();
    }

    @Test
    public void testWeldStartup() throws Exception {
        assertThat(container.isRunning(), is(true));
    }
}
