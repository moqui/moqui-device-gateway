package org.moqui.device.gateway;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Fast/local Quarkus test profile with no external infrastructure.
 *
 * Uses isolated in-memory H2 datasources for transactional and log storage and
 * disables broker-backed / PLC-backed consumers so tests stay hermetic.
 */
public class LocalNoInfraTestProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.ofEntries(
            Map.entry("quarkus.datasource.db-kind", "h2"),
            Map.entry("quarkus.datasource.jdbc.url", "jdbc:h2:mem:gateway;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE"),
            Map.entry("quarkus.datasource.username", "sa"),
            Map.entry("quarkus.datasource.password", ""),
            Map.entry("quarkus.datasource.log.db-kind", "h2"),
            Map.entry("quarkus.datasource.log.jdbc.url", "jdbc:h2:mem:gateway-log;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE"),
            Map.entry("quarkus.datasource.log.username", "sa"),
            Map.entry("quarkus.datasource.log.password", ""),
            Map.entry("gateway.startup.discovery.enabled", "false"),
            Map.entry("mqtt.read.route.autoStartup", "false"),
            Map.entry("mqtt.read.consume.uri", "seda:mqtt-read-device-request-in"),
            Map.entry("opcua.read.route.autoStartup", "false"),
            Map.entry("plc.log.route.autoStartup", "false"),
            Map.entry("file.transfer.uri", "seda:export-device-config-out?waitForTaskToComplete=Never"),
            Map.entry("file.transfer.uri.2.enabled", "false")
        );
    }
}
