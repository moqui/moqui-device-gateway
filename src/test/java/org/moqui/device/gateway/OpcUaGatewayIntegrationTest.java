package org.moqui.device.gateway;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.awaitility.Awaitility;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.moqui.device.gateway.service.GatewayRequestService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(OpcUaIntegrationTestProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OpcUaGatewayIntegrationTest {

    private static final String TEST_SUFFIX = UUID.randomUUID().toString().substring(0, 8).toUpperCase();

    private static final String FEEDBACK_ITEM_ID = "virtual_plc_feedback";
    private static final String FAULT_ITEM_ID = "virtual_plc_fault";
    private static final String REFERENCE_WRITE_ITEM_ID = "virtual_plc_reference_write";

    private int opcuaPort;
    private MiloTestServer miloTestServer;

    @Inject CamelContext camelContext;
    @Inject ProducerTemplate producer;
    @Inject AgroalDataSource dataSource;
    @Inject GatewayRequestService gatewayRequestService;

    @BeforeAll
    void setupOpcUaServer() throws Exception {
        opcuaPort = reserveTcpPort();
        miloTestServer = new MiloTestServer(opcuaPort);
    }

    @AfterAll
    void shutdownOpcUaServer() throws Exception {
        if (miloTestServer != null) {
            miloTestServer.shutdown();
        }
    }

    @BeforeEach
    void seedData() throws Exception {
        deleteSeedData();
        runSeedSql("device-gateway-seed.sql");
        runSeedSql("device-gateway-opcua-seed.sql");
    }

    @AfterEach
    void cleanup() throws Exception {
        removeDynamicRoutes();
        deleteSeedData();
    }

    @Test
    @Order(1)
    void opcuaSubscribeIngestsLiveNodeUpdatesIntoParameterRows() throws Exception {
        GatewayRequestService.RequestContext subscribeCtx = gatewayRequestService.loadRequestContext(seed("TestPlcOpcUaReadReq"));
        @SuppressWarnings("unchecked")
        Map<String, Object> result = producer.requestBody(
            "direct:dispatch-device-request",
            subscribeCtx,
            Map.class
        );

        assertEquals("completed", result.get("status"));
        assertEquals("opcua-subscribe-device-request", result.get("routeId"));

        miloTestServer.pushValue(FEEDBACK_ITEM_ID, 77.7);
        miloTestServer.pushValue(FAULT_ITEM_ID, "Y");

        Awaitility.await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertEquals(77.7, fetchParameterNumericValue(seed("TestPlcFeedbackParam")), 0.001);
                assertEquals("Y", fetchParameterSymbolicValue(seed("TestPlcFaultParam")));
            });

        Map<String, Object> unsubscribeResult = gatewayRequestService.unsubscribe(
            gatewayRequestService.loadRequestContext(seed("TestPlcOpcUaUnsubscribeReq")));

        assertEquals("completed", unsubscribeResult.get("status"));
        assertEquals(seed("TestPlcOpcUaReadReq"), unsubscribeResult.get("targetRequestName"));
        assertTrue(((List<?>) unsubscribeResult.get("routeIdList")).size() >= 2);
    }

    @Test
    @Order(2)
    void opcuaOneShotReadFetchesCurrentNodeValueIntoParameter() throws Exception {
        miloTestServer.pushValue(FEEDBACK_ITEM_ID, 55.5);

        GatewayRequestService.RequestContext readCtx = gatewayRequestService.loadRequestContext(seed("TestPlcOpcUaOneshotReq"));
        @SuppressWarnings("unchecked")
        Map<String, Object> result = producer.requestBody(
            "direct:dispatch-device-request",
            readCtx,
            Map.class
        );

        assertEquals("completed", result.get("status"));
        assertEquals("opcua-read-device-request", result.get("routeId"));
        assertEquals(55.5, fetchParameterNumericValue(seed("TestPlcFeedbackParam")), 0.001);
    }

    @Test
    @Order(3)
    void opcuaWritePublishesCurrentParameterValueToServerNode() throws Exception {
        updateParameterNumericValue(seed("TestPlcReferenceParam"), 88.8);

        GatewayRequestService.RequestContext writeCtx = gatewayRequestService.loadRequestContext(seed("TestPlcOpcUaWriteReq"));
        @SuppressWarnings("unchecked")
        Map<String, Object> result = producer.requestBody(
            "direct:dispatch-device-request",
            writeCtx,
            Map.class
        );

        assertEquals("completed", result.get("status"));
        assertEquals("opcua-write-device-request", result.get("routeId"));

        Awaitility.await()
            .atMost(Duration.ofSeconds(5))
            .pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> {
                DataValue written = miloTestServer.getLastWrittenValue(REFERENCE_WRITE_ITEM_ID);
                assertNotNull(written, "No write received by test server yet");
                assertEquals(88.8, toDouble(written), 0.001);
            });
    }

    // -------------------------------------------------------------------------
    // Seed helpers
    // -------------------------------------------------------------------------

    private void runSeedSql(String resourceName) throws Exception {
        int namespaceIndex = miloTestServer.getNamespaceIndex();
        String sql = resourceText(resourceName)
            .replace("__SUFFIX__", TEST_SUFFIX)
            .replace("__OPCUA_TRANSPORT_CONFIG__", "127.0.0.1:" + opcuaPort + "/milo")
            .replace("__OPCUA_FEEDBACK_NODE__", "ns=" + namespaceIndex + ";s=" + FEEDBACK_ITEM_ID)
            .replace("__OPCUA_FAULT_NODE__", "ns=" + namespaceIndex + ";s=" + FAULT_ITEM_ID)
            .replace("__OPCUA_REFERENCE_WRITE_NODE__", "ns=" + namespaceIndex + ";s=" + REFERENCE_WRITE_ITEM_ID);
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            for (String stmt : splitSqlStatements(sql)) {
                if (!stmt.isEmpty()) st.execute(stmt);
            }
        }
    }

    private List<String> splitSqlStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '\'') {
                current.append(ch);
                if (inString && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    current.append(sql.charAt(++i));
                } else {
                    inString = !inString;
                }
            } else if (ch == ';' && !inString) {
                String trimmed = current.toString().trim();
                if (!trimmed.isEmpty()) statements.add(trimmed);
                current.setLength(0);
            } else {
                current.append(ch);
            }
        }
        String trimmed = current.toString().trim();
        if (!trimmed.isEmpty()) statements.add(trimmed);
        return statements;
    }

    private void deleteSeedData() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            String suffixLike = "%_" + TEST_SUFFIX;
            st.executeUpdate("DELETE FROM PARAMETER_LOG WHERE PARAMETER_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM DEVICE_RULE WHERE DEVICE_RULE_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM DEVICE_RULE_SET WHERE DEVICE_RULE_SET_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM DEVICE_REQUEST_ITEM WHERE REQUEST_NAME LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM DEVICE_REQUEST WHERE REQUEST_NAME LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM DEVICE_CONNECTION WHERE CONNECTION_NAME LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM DEVICE_GROUP_MEMBER WHERE DEVICE_ID LIKE '" + suffixLike + "' OR MEMBER_DEVICE_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM PARAMETER WHERE PARAMETER_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM DEVICE_CONFIG WHERE DEVICE_CONFIG_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM TRAJECTORY_POINT WHERE APPROXIMATED_FUNCTION_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM TRAJECTORY WHERE APPROXIMATED_FUNCTION_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM PARAMETRIC_PATH_POINT WHERE APPROXIMATED_FUNCTION_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM APPROXIMATED_FUNCTION_SAMPLE WHERE APPROXIMATED_FUNCTION_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM PARAMETRIC_PATH WHERE APPROXIMATED_FUNCTION_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM VECTOR_COMPONENT WHERE VECTOR_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM VECTOR WHERE VECTOR_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM APPROXIMATED_FUNCTION WHERE APPROXIMATED_FUNCTION_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM PARAMETER_DEF WHERE PARAMETER_DEF_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM DEVICE_GROUP WHERE DEVICE_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM PHYSICAL_DEVICE WHERE DEVICE_ID LIKE '" + suffixLike + "'");
            st.executeUpdate("DELETE FROM DEVICE WHERE DEVICE_ID LIKE '" + suffixLike + "'");
        }
    }

    private void removeDynamicRoutes() throws Exception {
        String routePrefix = "device-request-consumer-" + seed("TestPlcOpcUaReadReq") + "-";
        for (String routeId : camelContext.getRoutes().stream().map(route -> route.getId()).toList()) {
            if (routeId.startsWith(routePrefix)) {
                camelContext.getRouteController().stopRoute(routeId);
                camelContext.removeRoute(routeId);
            }
        }
    }

    // -------------------------------------------------------------------------
    // DB assertions
    // -------------------------------------------------------------------------

    private double fetchParameterNumericValue(String parameterId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT NUMERIC_VALUE FROM PARAMETER WHERE PARAMETER_ID = ?")) {
            ps.setString(1, parameterId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getDouble(1);
            }
        }
    }

    private String fetchParameterSymbolicValue(String parameterId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT SYMBOLIC_VALUE FROM PARAMETER WHERE PARAMETER_ID = ?")) {
            ps.setString(1, parameterId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    private void updateParameterNumericValue(String parameterId, double numericValue) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "UPDATE PARAMETER SET NUMERIC_VALUE = ?, LAST_UPDATED_STAMP = CURRENT_TIMESTAMP WHERE PARAMETER_ID = ?")) {
            ps.setDouble(1, numericValue);
            ps.setString(2, parameterId);
            ps.executeUpdate();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private double toDouble(DataValue dataValue) {
        Object val = dataValue.getValue() instanceof Variant v ? v.getValue() : null;
        if (val instanceof Number n) return n.doubleValue();
        throw new IllegalArgumentException("Cannot convert DataValue to double: " + dataValue);
    }

    private String seed(String base) {
        return base + "_" + TEST_SUFFIX;
    }

    private int reserveTcpPort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    private String resourceText(String resourceName) throws IOException {
        var stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        assertNotNull(stream, "Missing resource: " + resourceName);
        try (stream) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
