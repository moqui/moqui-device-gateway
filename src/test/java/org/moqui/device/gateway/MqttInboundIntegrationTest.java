package org.moqui.device.gateway;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.awaitility.Awaitility;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.junit.jupiter.api.*;

import io.agroal.api.AgroalDataSource;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test: MQTT client publish -> ActiveMQ Artemis -> Camel -> PARAMETER_LOG.
 *
 * Prerequisite infrastructure:
 * - PostgreSQL initialized with the local test schema/seed or an equivalent Moqui DB.
 * - ActiveMQ Artemis from the standard Moqui Docker setup:
 *     docker compose -f ../moqui-framework/docker/activemq-compose.yml -p moqui-gateway up -d
 *
 * mosquitto_pub can be used as an MQTT client CLI for manual testing, but the broker is ActiveMQ Artemis.
 */
@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MqttInboundIntegrationTest {
    private static final String TEST_PARAMETER_DEF_ID = "TestMqttInboundReferenceDef";

    // Artemis MQTT connection parameters for the standard local integration setup
    private static final String MQTT_BROKER    = "tcp://localhost:1883";
    private static final String MQTT_USER      = "artemis";
    private static final String MQTT_PASSWORD  = "artemis";
    private static final String MQTT_TOPIC_IN  = "mqtt-read-device-request/parameter-log/in";

    // Test data — use unique suffix so parallel runs don't collide
    private static final String TEST_RUN_ID   = UUID.randomUUID().toString().substring(0, 8);
    private static final String PARAMETER_ID  = "TEST_PARAM_" + TEST_RUN_ID;

    @Inject
    AgroalDataSource dataSource;

    // -------------------------------------------------------------------------
    // Lifecycle — create minimal PARAMETER rows required by the FK constraint
    // on PARAMETER_LOG.parameter_id → PARAMETER.parameter_id
    // -------------------------------------------------------------------------

    @BeforeAll
    void createTestParameters() throws Exception {
        List<String> testPids = List.of(
            PARAMETER_ID + "_TC01",
            PARAMETER_ID + "_TC02",
            PARAMETER_ID + "_TC04"
        );
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO PARAMETER_DEF " +
                    "(PARAMETER_DEF_ID, PARAMETER_TYPE_ENUM_ID, PARAMETER_CODE, PARAMETER_NAME) " +
                    "VALUES (?, 'PtNumberFloat', 'TestMqttInboundReference', 'Test MQTT Inbound Reference') " +
                    "ON CONFLICT (PARAMETER_DEF_ID) DO NOTHING")) {
                ps.setString(1, TEST_PARAMETER_DEF_ID);
                ps.executeUpdate();
            }
            for (String pid : testPids) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO PARAMETER (PARAMETER_ID, PARAMETER_DEF_ID) " +
                        "VALUES (?, ?) ON CONFLICT DO NOTHING")) {
                    ps.setString(1, pid);
                    ps.setString(2, TEST_PARAMETER_DEF_ID);
                    ps.executeUpdate();
                }
            }
        }
    }

    @AfterAll
    void deleteTestParameters() throws Exception {
        List<String> testPids = List.of(
            PARAMETER_ID + "_TC01",
            PARAMETER_ID + "_TC02",
            PARAMETER_ID + "_TC04"
        );
        try (Connection conn = dataSource.getConnection()) {
            for (String pid : testPids) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM PARAMETER WHERE PARAMETER_ID = ?")) {
                    ps.setString(1, pid);
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM PARAMETER_DEF WHERE PARAMETER_DEF_ID = ? " +
                    "AND NOT EXISTS (SELECT 1 FROM PARAMETER WHERE PARAMETER_DEF_ID = ?)")) {
                ps.setString(1, TEST_PARAMETER_DEF_ID);
                ps.setString(2, TEST_PARAMETER_DEF_ID);
                ps.executeUpdate();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Creates a Paho MQTT5 client connected to Artemis. Equivalent to a CLI MQTT publisher. */
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

    private void publish(MqttClient client, String topic, String payload) throws Exception {
        MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(1);
        client.publish(topic, msg);
    }

    /** Counts PARAMETER_LOG rows matching parameterId. */
    private int countParameterLogRows(String parameterId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT COUNT(*) FROM public.PARAMETER_LOG WHERE PARAMETER_ID = ?")) {
            ps.setString(1, parameterId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    /** Reads the latest PARAMETER_LOG row for parameterId. */
    private ResultRow fetchLatestRow(String parameterId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT PARAMETER_LOG_ID, PARAMETER_ID, NUMERIC_VALUE, OBSERVED_DATE " +
                 "FROM public.PARAMETER_LOG WHERE PARAMETER_ID = ? " +
                 "ORDER BY OBSERVED_DATE DESC LIMIT 1")) {
            ps.setString(1, parameterId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new ResultRow(
                    rs.getString("PARAMETER_LOG_ID"),
                    rs.getString("PARAMETER_ID"),
                    rs.getDouble("NUMERIC_VALUE"),
                    rs.getTimestamp("OBSERVED_DATE") != null
                );
            }
        }
    }

    /** Cleans up test rows after each test. */
    private void deleteTestRows(String parameterId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "DELETE FROM public.PARAMETER_LOG WHERE PARAMETER_ID = ?")) {
            ps.setString(1, parameterId);
            ps.executeUpdate();
        }
    }

    record ResultRow(String logId, String parameterId, double numericValue, boolean hasTimestamp) {}

    // =========================================================================
    // Test cases
    // =========================================================================

    /**
     * TC-01: Single MQTT message → Camel inbound route → INSERT PARAMETER_LOG.
     *
     * Equivalent manual CLI:
     *   mosquitto_pub -h localhost -p 1883 -u artemis -P artemis \
     *     -t mqtt-read-device-request/parameter-log/in -m '{"parameterId":"TEST_PARAM_xxx","numericValue":42.5}'
     */
    @Test
    @Order(1)
    void tc01_singleMessageInsertsParameterLog() throws Exception {
        String pid = PARAMETER_ID + "_TC01";
        String payload = String.format(
            "{\"parameterId\":\"%s\",\"numericValue\":42.5}", pid);

        MqttClient pub = mqttPublisher("test-pub-tc01-" + TEST_RUN_ID);
        try {
            publish(pub, MQTT_TOPIC_IN, payload);
        } finally {
            pub.disconnect();
            pub.close();
        }

        // Wait up to 10s for Camel to process and INSERT
        Awaitility.await()
            .alias("TC-01: PARAMETER_LOG row inserted")
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> assertEquals(1, countParameterLogRows(pid),
                "Expected exactly 1 row in PARAMETER_LOG for " + pid));

        ResultRow row = fetchLatestRow(pid);
        assertNotNull(row, "Row must be present");
        assertNotNull(row.logId(), "parameterLogId must be assigned by the route or persistence layer");
        assertFalse(row.logId().isBlank(), "parameterLogId must not be blank");
        assertEquals(pid, row.parameterId());
        assertEquals(42.5, row.numericValue(), 0.001);
        assertTrue(row.hasTimestamp(), "OBSERVED_DATE must be set");

        System.out.printf("[TC-01] OK — PARAMETER_LOG_ID=%s, numericValue=%.1f%n",
            row.logId(), row.numericValue());

        deleteTestRows(pid);
    }

    /**
     * TC-02: Message without parameterLogId — the route/persistence layer assigns it.
     *
     * This test verifies the inserted row has a non-null ID even when the incoming
     * MQTT payload omits parameterLogId.
     */
    @Test
    @Order(2)
    void tc02_parameterLogIdIsAutoGeneratedByDatabase() throws Exception {
        String pid = PARAMETER_ID + "_TC02";
        String payload = String.format(
            "{\"parameterId\":\"%s\",\"numericValue\":99.0}", pid);

        MqttClient pub = mqttPublisher("test-pub-tc02-" + TEST_RUN_ID);
        try {
            publish(pub, MQTT_TOPIC_IN, payload);
        } finally {
            pub.disconnect();
            pub.close();
        }

        Awaitility.await()
            .alias("TC-02: row with DB-auto-generated ID")
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> assertEquals(1, countParameterLogRows(pid)));

        ResultRow row = fetchLatestRow(pid);
        assertNotNull(row);
        assertNotNull(row.logId(), "PARAMETER_LOG_ID must be assigned");
        assertFalse(row.logId().isBlank(), "PARAMETER_LOG_ID must not be blank");

        System.out.printf("[TC-02] OK — assigned PARAMETER_LOG_ID=%s%n", row.logId());
        deleteTestRows(pid);
    }

    /**
     * TC-04: Burst of 10 messages — all must be inserted without loss.
     *
     * Tests concurrent inserts with auto-generated UUIDs (no PK conflict).
     */
    @Test
    @Order(4)
    void tc04_burstOf10MessagesAllInserted() throws Exception {
        String pid = PARAMETER_ID + "_TC04";
        int count  = 10;

        MqttClient pub = mqttPublisher("test-pub-tc04-" + TEST_RUN_ID);
        try {
            for (int i = 0; i < count; i++) {
                String payload = String.format(
                    "{\"parameterId\":\"%s\",\"numericValue\":%.1f}", pid, (double) i);
                publish(pub, MQTT_TOPIC_IN, payload);
            }
        } finally {
            pub.disconnect();
            pub.close();
        }

        Awaitility.await()
            .alias("TC-04: all " + count + " rows inserted")
            .atMost(Duration.ofSeconds(20))
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> assertEquals(count, countParameterLogRows(pid),
                "Expected " + count + " rows in PARAMETER_LOG for " + pid));

        System.out.printf("[TC-04] OK — %d rows inserted for parameterId=%s%n", count, pid);
        deleteTestRows(pid);
    }

}
