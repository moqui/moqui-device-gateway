package org.moqui.device.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;

/**
 * Integration test: MQTT → plc-log-consumer → plc-log-ingest → PARAMETER_LOG / DEVICE_LOG.
 *
 * Simulates the CODESYS LoggerFacade MQTT appender publishing batches in the
 * numbered-object format: {"1":{loggerName, source, type, numericValue|message, ...}, ...}
 *
 * Requires ActiveMQ Artemis from the standard moqui-framework Docker setup,
 * or an equivalent MQTT broker reachable through the configured brokerUri.
 *
 * Standard infrastructure:
 *   docker compose -f docker/postgres-compose.yml -p moqui-gateway up -d
 *   docker compose -f ../moqui-framework/docker/activemq-compose.yml -p moqui-gateway up -d
 *
 * Run:
 *   ./gradlew test --tests '*PlcLogIngestIntegrationTest' -Dquarkus.profile=integration
 */
@QuarkusTest
@TestProfile(PlcLogIngestTestProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PlcLogIngestIntegrationTest {

    private static final String MQTT_BROKER   = "tcp://localhost:1883";
    private static final String MQTT_USER     = "artemis";
    private static final String MQTT_PASSWORD = "artemis";
    private static final String PLC_LOG_TOPIC = PlcLogIngestTestProfile.PLC_LOG_TOPIC;

    // Unique logger name per test run so rows don't collide across parallel runs
    private static final String TEST_RUN_ID  = UUID.randomUUID().toString().substring(0, 8);
    private static final String LOGGER_NAME  = "hvac_" + TEST_RUN_ID;

    @Inject
    AgroalDataSource dataSource;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @BeforeEach
    void ensureLoggerDeviceExists() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.executeUpdate(
                "INSERT INTO DEVICE (DEVICE_ID, DEVICE_TYPE_ENUM_ID) " +
                "VALUES ('" + LOGGER_NAME + "', 'DtComputer') " +
                "ON CONFLICT (DEVICE_ID) DO NOTHING");
            st.executeUpdate(
                "INSERT INTO PHYSICAL_DEVICE (DEVICE_ID, DEVICE_NAME) " +
                "VALUES ('" + LOGGER_NAME + "', '" + LOGGER_NAME + "') " +
                "ON CONFLICT (DEVICE_ID) DO NOTHING");
        }
    }

    @AfterEach
    void cleanup() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM PARAMETER_LOG WHERE PARAMETER_ID LIKE '" + LOGGER_NAME + ".%'");
            st.executeUpdate("DELETE FROM DEVICE_LOG    WHERE DEVICE_ID = '" + LOGGER_NAME + "'");
            st.executeUpdate("DELETE FROM PARAMETER     WHERE PARAMETER_ID LIKE '" + LOGGER_NAME + ".%'");
            st.executeUpdate("DELETE FROM PHYSICAL_DEVICE WHERE DEVICE_ID = '" + LOGGER_NAME + "'");
            st.executeUpdate("DELETE FROM DEVICE WHERE DEVICE_ID = '" + LOGGER_NAME + "'");
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private MqttClient mqttPublisher(String clientId) throws Exception {
        MqttClient client = new MqttClient(MQTT_BROKER, clientId, null);
        MqttConnectionOptions opts = new MqttConnectionOptions();
        opts.setUserName(MQTT_USER);
        opts.setPassword(MQTT_PASSWORD.getBytes(StandardCharsets.UTF_8));
        opts.setCleanStart(true);
        opts.setConnectionTimeout(10);
        client.connect(opts);
        return client;
    }

    private void publish(MqttClient client, String payload) throws Exception {
        MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(1);
        client.publish(PLC_LOG_TOPIC, msg);
    }

    private int countParameterLogRows(String parameterId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT COUNT(*) FROM PARAMETER_LOG WHERE PARAMETER_ID = ?")) {
            ps.setString(1, parameterId);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); return rs.getInt(1); }
        }
    }

    private int countDeviceLogRows(String deviceId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT COUNT(*) FROM DEVICE_LOG WHERE DEVICE_ID = ?")) {
            ps.setString(1, deviceId);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); return rs.getInt(1); }
        }
    }

    /** Builds a single-entry LoggerFacade batch JSON (type 1 = numeric). */
    private String numericBatch(String source, double value) {
        return String.format(
            "{\"1\":{\"logEventDate\":\"DT#2026-05-25-10:00:00\","
            + "\"loggerName\":\"%s\",\"source\":\"%s\","
            + "\"type\":1,\"repeatCount\":1,\"numericValue\":%s}}",
            LOGGER_NAME, source, value);
    }

    /** Builds a single-entry LoggerFacade batch JSON (type 0 = text message). */
    private String textBatch(String source, String message) {
        return String.format(
            "{\"1\":{\"logEventDate\":\"DT#2026-05-25-10:00:01\","
            + "\"loggerName\":\"%s\",\"source\":\"%s\","
            + "\"type\":0,\"repeatCount\":1,\"message\":\"%s\"}}",
            LOGGER_NAME, source, message);
    }

    /** Builds a single-entry LoggerFacade batch JSON with empty source (no-source → DEVICE_LOG). */
    private String noSourceBatch(String message) {
        return String.format(
            "{\"1\":{\"logEventDate\":\"DT#2026-05-25-10:00:02\","
            + "\"loggerName\":\"%s\",\"source\":\"\","
            + "\"type\":0,\"repeatCount\":1,\"message\":\"%s\"}}",
            LOGGER_NAME, message);
    }

    // =========================================================================
    // Test cases
    // =========================================================================

    /**
     * TC-PLC-01: Source entry with numeric value → PARAMETER_LOG row with numericValue.
     *
     * The route must:
     *   1. Ensure PlcLoggerDef PARAMETER_DEF row.
     *   2. Ensure PARAMETER row for loggerName.source.
     *   3. Insert into PARAMETER_LOG with numericValue.
     */
    @Test
    @Order(1)
    void tc01_numericSourceEntryInsertsParameterLogWithNumericValue() throws Exception {
        String source = "tempSensor";
        String parameterId = LOGGER_NAME + "." + source;

        MqttClient pub = mqttPublisher("plc-log-tc01-" + TEST_RUN_ID);
        try {
            publish(pub, numericBatch(source, 21.5));
        } finally {
            pub.disconnect(); pub.close();
        }

        Awaitility.await()
            .alias("TC-PLC-01: PARAMETER_LOG row with numericValue=21.5")
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> assertEquals(1, countParameterLogRows(parameterId)));

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT NUMERIC_VALUE FROM PARAMETER_LOG WHERE PARAMETER_ID = ? LIMIT 1")) {
            ps.setString(1, parameterId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(21.5, rs.getDouble(1), 0.001);
            }
        }
    }

    /**
     * TC-PLC-02: Source entry with text message → PARAMETER_LOG row with symbolicValue.
     */
    @Test
    @Order(2)
    void tc02_textSourceEntryInsertsParameterLogWithSymbolicValue() throws Exception {
        String source = "stateLogger";
        String parameterId = LOGGER_NAME + "." + source;

        MqttClient pub = mqttPublisher("plc-log-tc02-" + TEST_RUN_ID);
        try {
            publish(pub, textBatch(source, "Standby state entered"));
        } finally {
            pub.disconnect(); pub.close();
        }

        Awaitility.await()
            .alias("TC-PLC-02: PARAMETER_LOG row with symbolicValue")
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> assertEquals(1, countParameterLogRows(parameterId)));

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT SYMBOLIC_VALUE FROM PARAMETER_LOG WHERE PARAMETER_ID = ? LIMIT 1")) {
            ps.setString(1, parameterId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("Standby state entered", rs.getString(1));
            }
        }
    }

    /**
     * TC-PLC-03: No-source entry → DEVICE_LOG row (deviceId = loggerName).
     */
    @Test
    @Order(3)
    void tc03_noSourceEntryInsertsDeviceLog() throws Exception {
        MqttClient pub = mqttPublisher("plc-log-tc03-" + TEST_RUN_ID);
        try {
            publish(pub, noSourceBatch("Framework started."));
        } finally {
            pub.disconnect(); pub.close();
        }

        Awaitility.await()
            .alias("TC-PLC-03: DEVICE_LOG row for deviceId=" + LOGGER_NAME)
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> assertTrue(countDeviceLogRows(LOGGER_NAME) >= 1));

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT PAYLOAD FROM DEVICE_LOG WHERE DEVICE_ID = ? LIMIT 1")) {
            ps.setString(1, LOGGER_NAME);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "DEVICE_LOG row must be present");
                String payload = rs.getString(1);
                assertNotNull(payload);
                assertTrue(payload.contains("Framework started."),
                    "DEVICE_LOG PAYLOAD must contain the original message text");
            }
        }
    }

    /**
     * TC-PLC-04: Mixed batch — source entries → PARAMETER_LOG, no-source entry → DEVICE_LOG.
     * Both paths are isolated: one must not suppress the other.
     */
    @Test
    @Order(4)
    void tc04_mixedBatchRoutesSourceEntriesToParameterLogAndNoSourceToDeviceLog() throws Exception {
        String source = "rhSensor";
        String parameterId = LOGGER_NAME + "." + source;

        String mixedBatch =
            "{\"1\":{\"logEventDate\":\"DT#2026-05-25-10:00:03\","
            + "\"loggerName\":\"" + LOGGER_NAME + "\",\"source\":\"" + source + "\","
            + "\"type\":1,\"repeatCount\":1,\"numericValue\":72.0},"
            + "\"2\":{\"logEventDate\":\"DT#2026-05-25-10:00:03\","
            + "\"loggerName\":\"" + LOGGER_NAME + "\",\"source\":\"\","
            + "\"type\":0,\"repeatCount\":1,\"message\":\"Cycle started.\"}}";

        MqttClient pub = mqttPublisher("plc-log-tc04-" + TEST_RUN_ID);
        try {
            publish(pub, mixedBatch);
        } finally {
            pub.disconnect(); pub.close();
        }

        Awaitility.await()
            .alias("TC-PLC-04: both PARAMETER_LOG and DEVICE_LOG rows inserted")
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertTrue(countParameterLogRows(parameterId) >= 1,
                    "Source entry must appear in PARAMETER_LOG");
                assertTrue(countDeviceLogRows(LOGGER_NAME) >= 1,
                    "No-source entry must appear in DEVICE_LOG");
            });
    }

    /**
     * TC-PLC-05: Timestamp in DT#YYYY-MM-DD-HH:MM:SS format is parsed and stored as OBSERVED_DATE.
     */
    @Test
    @Order(5)
    void tc05_dtHashTimestampIsParsedIntoObservedDate() throws Exception {
        String source = "clock";
        String parameterId = LOGGER_NAME + "." + source;

        String batch =
            "{\"1\":{\"logEventDate\":\"DT#2026-01-15-08:30:00\","
            + "\"loggerName\":\"" + LOGGER_NAME + "\",\"source\":\"" + source + "\","
            + "\"type\":1,\"repeatCount\":1,\"numericValue\":55.0}}";

        MqttClient pub = mqttPublisher("plc-log-tc05-" + TEST_RUN_ID);
        try {
            publish(pub, batch);
        } finally {
            pub.disconnect(); pub.close();
        }

        Awaitility.await()
            .alias("TC-PLC-05: OBSERVED_DATE parsed from DT# format")
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> assertEquals(1, countParameterLogRows(parameterId)));

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT OBSERVED_DATE FROM PARAMETER_LOG WHERE PARAMETER_ID = ? LIMIT 1")) {
            ps.setString(1, parameterId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                java.sql.Timestamp ts = rs.getTimestamp(1);
                assertNotNull(ts, "OBSERVED_DATE must be set when DT# format parses correctly");
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.setTimeInMillis(ts.getTime());
                assertEquals(2026, cal.get(java.util.Calendar.YEAR));
                assertEquals(java.util.Calendar.JANUARY, cal.get(java.util.Calendar.MONTH));
                assertEquals(15, cal.get(java.util.Calendar.DAY_OF_MONTH));
            }
        }
    }

    /**
     * TC-PLC-06: Malformed batch JSON is discarded; the consumer route keeps running.
     */
    @Test
    @Order(6)
    void tc06_malformedBatchIsDiscardedWithoutStoppingConsumerRoute() throws Exception {
        MqttClient pub = mqttPublisher("plc-log-tc06-" + TEST_RUN_ID);
        try {
            publish(pub, "{not-valid-json");
        } finally {
            pub.disconnect(); pub.close();
        }

        // Give the route a moment to process the bad message — it must survive
        Thread.sleep(500);

        // Publish a valid message after the bad one; it must still be processed
        String source = "afterBad";
        String parameterId = LOGGER_NAME + "." + source;
        MqttClient pub2 = mqttPublisher("plc-log-tc06b-" + TEST_RUN_ID);
        try {
            publish(pub2, numericBatch(source, 1.0));
        } finally {
            pub2.disconnect(); pub2.close();
        }

        Awaitility.await()
            .alias("TC-PLC-06: route survives malformed batch; valid message still processed")
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> assertEquals(1, countParameterLogRows(parameterId),
                "Route must still process valid messages after a malformed batch is discarded"));
    }
}
