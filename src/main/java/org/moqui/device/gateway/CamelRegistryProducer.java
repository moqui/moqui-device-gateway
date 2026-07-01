package org.moqui.device.gateway;

import javax.sql.DataSource;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;

/**
 * Registers Camel datasource aliases over the two Quarkus datasources used by the
 * industrial stack:
 *  - {@code moquiDataSource} → transactional PostgreSQL datasource
 *  - {@code moquiLogDataSource} → telemetry/log datasource, typically TimescaleDB
 *
 * SQL route URIs reference one of these by name:
 *   sql:{{query}}?dataSource=#{{camel.sql.datasource}} (transactional DB)
 *   sql:{{query}}?dataSource=#{{camel.sql.log.datasource}} (log DB)
 *
 * We inject {@code AgroalDataSource} rather than plain {@code DataSource} to avoid CDI
 * ambiguity: a {@code @Produces @Named} method on plain {@code DataSource} would create a
 * second {@code @Default} bean alongside the synthetic Agroal bean, causing
 * {@code AmbiguousResolutionException} at boot.
 */
@ApplicationScoped
public class CamelRegistryProducer {

    @Inject
    AgroalDataSource agroalDataSource;

    @Inject
    @io.quarkus.agroal.DataSource("log")
    AgroalDataSource logAgroalDataSource;

    @Produces
    @Named("moquiDataSource")
    DataSource moquiDataSource() {
        return agroalDataSource;
    }

    @Produces
    @Named("moquiLogDataSource")
    DataSource moquiLogDataSource() {
        return logAgroalDataSource;
    }
}
