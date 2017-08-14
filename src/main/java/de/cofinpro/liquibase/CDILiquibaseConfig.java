package de.cofinpro.liquibase;

import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Configures liquibase CDI integration.
 * <p>
 * All you have to do is implement this class inside a CDI-enabled archive and liquibase will pick it up
 * and run your changesets.
 * <p>
 * This implementation supports schema-based multi-tenancy. You can specify multiple schemas by using
 * {@link #getSchemas()}.
 *
 * @author Gregor Tudan, Cofinpro AG
 */
public abstract class CDILiquibaseConfig {

    /**
     * Specifies the path to the changelog file. Resources will be resolved using the {@linkplain ResourceAccessor}
     * returned by {@link #getResourceAccessor()}.
     *
     * @return the path to the changelog relative to the resource accessor
     */
    public abstract String getChangeLog();

    /**
     * @return the datasource to use for connecting to the database
     */
    public abstract DataSource getDataSource();

    /**
     * return the resource accessor used by liquibase to acquire the changelogs or supplementary files (i.e. CSVs)
     * <p>
     * This defaults to classloader of this class
     *
     * @return a resource accessor
     */
    public ResourceAccessor getResourceAccessor() {
        return new ClassLoaderResourceAccessor(getClass().getClassLoader());
    }

    /**
     * Configures the default schema.
     * <p>
     * This setting will be overridden by {@link #getSchemas()}, so only use it when running without
     * multi-tenancy
     *
     * @return the default schema to use. When returing {@code null} liquibase will figure out the default schema based
     * on the database defaults.
     */
    public String getDefaultSchema() {
        return null;
    }

    /**
     * All schemas that should be updated by liquibase.
     * <p>
     * This allows running in schema based multi-tenancy environments. This overrides the {@link #getDefaultSchema()}
     * setting.
     * <p>
     * Schemas will get processed sequentially in the order they appear in the collection. If an update fails, the
     * update of the following schemas will be aborted.
     * <p>
     * Keep in mind that this  <em>does not</em> override any schema configurations in your changeset. If you specify
     * a change there for a specific schema, the change will get executed for every schema in this collection!
     *
     * @return a list of all schema names that should be updated. If null or empty, only the default schema will be used.
     */
    public Collection<String> getSchemas() {
        return Collections.emptyList();
    }

    /**
     * Allows using different credentials for schemas when updating multiple schemas.
     * <p>
     * This expects a map with the schema name as key and an array with the username as first and the password as
     * second entry. If no entry is found for a schema or the array is of wrong size, the default credentials for
     * this datasource will be used.
     *
     * @return a map with the schema name as key and the credentials as value.
     */
    public Map<String, String[]> getSchemaCredentials() {
        return Collections.emptyMap();
    }

    /**
     * @return a map of parameters used to configure the changeset
     */
    public Map<String, String> getParameters() {
        return Collections.emptyMap();
    }

    /**
     *
     * @return a label expression specifying which changes to run
     */
    public String getLabels() {
        return null;
    }

    /**
     * @return a list of contexts specifying which changes to run
     */
    public Collection<String> getContexts() {
        return Collections.emptyList();
    }

    /**
     * @return if all objects in the schema should be deleted before performing the update
     */
    public boolean isDropFirst() {
        return false;
    }
}


