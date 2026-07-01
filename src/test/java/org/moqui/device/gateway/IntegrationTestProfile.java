package org.moqui.device.gateway;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Quarkus test profile that activates the %integration section in application.properties.
 *
 * Requires live infrastructure:
 *   - PostgreSQL localhost:5432, initialized with docker/postgres-compose.yml or equivalent
 *   - MQTT broker localhost:1883
 *   - OPC UA test server for OPC UA tests
 */
public class IntegrationTestProfile implements QuarkusTestProfile {

    @Override
    public String getConfigProfile() {
        return "integration";
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
            "gateway.device.id", "GW_EDGE_01",
            "plc.log.route.autoStartup", "false",
            "mqtt.read.topic", "mqtt-read-device-request/parameter-log/in",
            "quarkus.http.port", "0",
            "quarkus.http.test-port", "0",
            "quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:5432/moqui",
            "quarkus.datasource.log.jdbc.url", "jdbc:postgresql://localhost:5432/moqui"
        );
    }
}
