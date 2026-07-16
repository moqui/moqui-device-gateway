package org.moqui.device.gateway;

import java.util.HashMap;
import java.util.Map;

/**
 * Test profile for PlcLogIngestIntegrationTest.
 *
 * Extends IntegrationTestProfile (PostgreSQL + Artemis on localhost) and
 * redirects the PLC log consumer to a test-specific topic on the local/integration MQTT broker.
 * The main MQTT read route is disabled to avoid interference.
 *
 * Prerequisite: same infrastructure as IntegrationTestProfile.
 * Run: ./gradlew integrationTest --tests '*PlcLogIngestIntegrationTest'
 */
public class PlcLogIngestTestProfile extends IntegrationTestProfile {

    static final String PLC_LOG_TOPIC = "plc-log-consumer/test-in";

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> overrides = new HashMap<>(super.getConfigOverrides());
        overrides.put("plc.log.route.autoStartup", "true");
        overrides.put("plc.log.consume.uri",
            "paho-mqtt5:" + PLC_LOG_TOPIC +
            "?brokerUrl=tcp://localhost:1883&clientId=camel-plc-log-test&userName=artemis&password=artemis");
        // Disable the main MQTT ingest route — only PLC log path is under test
        overrides.put("mqtt.read.route.autoStartup", "false");
        overrides.put("mqtt.read.consume.uri", "seda:mqtt-read-device-request-in");
        return overrides;
    }
}
