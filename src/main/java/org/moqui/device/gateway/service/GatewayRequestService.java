package org.moqui.device.gateway.service;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.milo.MiloConstants;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.agroal.api.AgroalDataSource;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;

@ApplicationScoped
@Named("gatewayRequestService")
public class GatewayRequestService {
    private static final Logger logger = Logger.getLogger(GatewayRequestService.class);
    private static final TypeReference<List<Object>> LIST_OBJECT = new TypeReference<>() {};
    private static final TypeReference<Map<String, Object>> MAP_OBJECT = new TypeReference<>() {};
    private static final int OPCUA_WRITE_RETRY_DELAY_MS = 200;
    private static final int OPCUA_WRITE_DEFAULT_TIMEOUT_MS = 5000;
    private static final String GATEWAY_DEVICE_EXISTS_SQL = """
        SELECT d.DEVICE_ID
        FROM DEVICE d
        JOIN PHYSICAL_DEVICE pd ON pd.DEVICE_ID = d.DEVICE_ID
        WHERE d.DEVICE_ID = ?
        """;
    private static final String GATEWAY_MEMBERSHIP_COUNT_SQL = """
        SELECT COUNT(*) AS CNT
        FROM DEVICE_GROUP_MEMBER dgm
        WHERE dgm.MEMBER_DEVICE_ID = ?
          AND dgm.PURPOSE_ENUM_ID = 'DgmpEdgeGateway'
          AND (dgm.STATUS_ID IS NULL OR dgm.STATUS_ID = 'DgmsAlive')
        """;
    private static final String GATEWAY_DEVICE_SCOPE_COUNT_SQL = """
        WITH RECURSIVE root_groups (DEVICE_ID) AS (
            SELECT dgm.DEVICE_ID AS DEVICE_ID
            FROM DEVICE_GROUP_MEMBER dgm
            WHERE dgm.MEMBER_DEVICE_ID = ?
              AND dgm.PURPOSE_ENUM_ID = 'DgmpEdgeGateway'
              AND (dgm.STATUS_ID IS NULL OR dgm.STATUS_ID = 'DgmsAlive')
        ), visible_devices (DEVICE_ID) AS (
            SELECT rg.DEVICE_ID
            FROM root_groups rg
            UNION
            SELECT dgm.MEMBER_DEVICE_ID AS DEVICE_ID
            FROM DEVICE_GROUP_MEMBER dgm
            JOIN root_groups rg ON rg.DEVICE_ID = dgm.DEVICE_ID
            WHERE (dgm.STATUS_ID IS NULL OR dgm.STATUS_ID = 'DgmsAlive')
            UNION
            SELECT child.MEMBER_DEVICE_ID AS DEVICE_ID
            FROM DEVICE_GROUP_MEMBER child
            JOIN visible_devices vd ON vd.DEVICE_ID = child.DEVICE_ID
            WHERE (child.STATUS_ID IS NULL OR child.STATUS_ID = 'DgmsAlive')
        )
        SELECT COUNT(*) AS CNT
        FROM (SELECT DISTINCT DEVICE_ID FROM visible_devices) scoped
        WHERE scoped.DEVICE_ID = ?
        """;
    private static final String STARTUP_SUBSCRIPTION_SQL = """
        WITH RECURSIVE root_groups (DEVICE_ID) AS (
            SELECT dgm.DEVICE_ID AS DEVICE_ID
            FROM DEVICE_GROUP_MEMBER dgm
            WHERE dgm.MEMBER_DEVICE_ID = ?
              AND dgm.PURPOSE_ENUM_ID = 'DgmpEdgeGateway'
              AND (dgm.STATUS_ID IS NULL OR dgm.STATUS_ID = 'DgmsAlive')
        ), scoped_members (DEVICE_ID, MEMBER_DEVICE_ID, PURPOSE_ENUM_ID, STATUS_ID) AS (
            SELECT dgm.DEVICE_ID, dgm.MEMBER_DEVICE_ID, dgm.PURPOSE_ENUM_ID, dgm.STATUS_ID
            FROM DEVICE_GROUP_MEMBER dgm
            JOIN root_groups rg ON rg.DEVICE_ID = dgm.DEVICE_ID
            WHERE (dgm.STATUS_ID IS NULL OR dgm.STATUS_ID = 'DgmsAlive')
            UNION
            SELECT child.DEVICE_ID, child.MEMBER_DEVICE_ID, child.PURPOSE_ENUM_ID, child.STATUS_ID
            FROM DEVICE_GROUP_MEMBER child
            JOIN scoped_members sm ON sm.MEMBER_DEVICE_ID = child.DEVICE_ID
            WHERE (child.STATUS_ID IS NULL OR child.STATUS_ID = 'DgmsAlive')
        )
        SELECT startup.REQUEST_NAME
        FROM (
            SELECT DISTINCT
                dr.REQUEST_NAME,
                COALESCE(dr.PRIORITY, 999999) AS SORT_PRIORITY,
                COALESCE(dr.SEQUENCE_NUM, 999999) AS SORT_SEQUENCE
            FROM scoped_members sm
            JOIN DEVICE_REQUEST dr
                ON dr.DEVICE_ID = sm.MEMBER_DEVICE_ID
            WHERE sm.PURPOSE_ENUM_ID IN (
                'DgmpProcessPLC',
                'DgmpSafetyPLC',
                'DgmpController',
                'DgmpMotionController',
                'DgmpRemoteIO',
                'DgmpSafetyRemoteIO',
                'DgmpExtremeConditionRemoteIO'
            )
              AND dr.ROUTER_ENUM_ID = 'DrrMoquiDeviceGateway'
              AND dr.REQUEST_TYPE_ENUM_ID IN (
                  'DrtSubscribe',
                  'DrtEvent',
                  'DrtStateChange',
                  'DrtCyclic'
              )
              AND (dr.FROM_DATE IS NULL OR dr.FROM_DATE <= CURRENT_TIMESTAMP)
              AND (dr.THRU_DATE IS NULL OR dr.THRU_DATE > CURRENT_TIMESTAMP)
        ) startup
        ORDER BY startup.SORT_PRIORITY, startup.SORT_SEQUENCE, startup.REQUEST_NAME
        """;

    private final ConcurrentHashMap<String, Object> subscriptionLocks = new ConcurrentHashMap<>();

    @Inject
    AgroalDataSource dataSource;

    @Inject
    CamelContext camelContext;

    @Inject
    ProducerTemplate producer;

    @Inject
    ObjectMapper objectMapper;

    @ConfigProperty(name = "gateway.request.sql")
    String requestSql;

    @ConfigProperty(name = "gateway.request.items.sql")
    String requestItemsSql;

    @ConfigProperty(name = "mqtt.write.afterPublish.enabled", defaultValue = "true")
    boolean mqttWriteAfterPublishEnabled;

    @ConfigProperty(name = "mqtt.write.afterPublish.uri", defaultValue = "seda:mqtt-write-device-request-audit?waitForTaskToComplete=Never")
    String mqttWriteAfterPublishUri;

    @ConfigProperty(name = "opcua.write.afterPublish.enabled", defaultValue = "true")
    boolean opcuaWriteAfterPublishEnabled;

    @ConfigProperty(name = "opcua.write.afterPublish.uri", defaultValue = "seda:opcua-write-device-request-audit?waitForTaskToComplete=Never")
    String opcuaWriteAfterPublishUri;

    @ConfigProperty(name = "device.config.export.fetch.uri")
    String deviceConfigExportFetchUri;

    @ConfigProperty(name = "gateway.device.id")
    Optional<String> gatewayDeviceId;

    @ConfigProperty(name = "gateway.startup.discovery.enabled", defaultValue = "true")
    boolean gatewayStartupDiscoveryEnabled;

    void onStart(@Observes StartupEvent ev) {
        if (!gatewayStartupDiscoveryEnabled) {
            logger.debug("Gateway startup DB discovery is disabled for this profile.");
            return;
        }
        final GatewayIdentityStatus identity;
        try {
            identity = inspectGatewayIdentity();
        } catch (RuntimeException e) {
            logger.errorf(e, "Gateway startup validation failed before route restore.");
            return;
        }
        if (!identity.ready()) {
            logger.errorf("Gateway startup validation failed: %s", identity.message());
            return;
        }

        List<String> requestNames = loadStartupSubscriptionRequestNames(identity.gatewayDeviceId());
        int restoredRequests = 0;
        int restoredRoutes = 0;
        for (String requestName : requestNames) {
            try {
                RequestContext context = loadRequestContext(requestName);
                Map<String, Object> result;
                if (isPlcLogContext(context)) {
                    result = subscribePlcLog(context);
                } else if (isOpcuaContext(context)) {
                    result = subscribeOpcUa(context);
                } else {
                    result = subscribeMqtt(context);
                }
                restoredRequests++;
                restoredRoutes += extractRouteCount(result);
                logger.infof("Restored DB-defined subscription for DeviceRequest %s on startup.", requestName);
            } catch (Exception e) {
                logger.warnf(e, "Failed to restore DB-defined subscription for DeviceRequest %s on startup.", requestName);
            }
        }
        logger.infof("Gateway %s restored %d DB-defined subscription route(s) from %d DeviceRequest row(s).",
            identity.gatewayDeviceId(), restoredRoutes, restoredRequests);
    }

    public Map<String, Object> dispatch(RequestContext context) {
        String requestType = context.requestTypeEnumId();
        if ("DrtRead".equals(requestType)) return runRead(context);
        if ("DrtWrite".equals(requestType) || "DrtConfigWrite".equals(requestType)) {
            return isOpcuaContext(context) ? runOpcUaWrite(context) : runMqttWrite(context);
        }
        if (isSubscribeRequestType(requestType)) {
            if (isPlcLogContext(context)) return subscribePlcLog(context);
            return isOpcuaContext(context) ? subscribeOpcUa(context) : subscribeMqtt(context);
        }
        if ("DrtUnsubscribe".equals(requestType)) return unsubscribe(context);
        throw new IllegalArgumentException("Unsupported gateway request type " + requestType
            + " for request " + context.requestName() + ".");
    }

    public Map<String, Object> runRead(RequestContext context) {
        assertRequestType(context, "DrtRead");
        if (!isOpcuaContext(context)) {
            throw new IllegalArgumentException("DeviceRequest " + context.requestName()
                + " is a direct read request, but only OPC UA one-shot reads are supported by moqui-device-gateway.");
        }

        List<Map<String, Object>> normalizedRows = new ArrayList<>();
        List<String> readUriList = new ArrayList<>();
        for (String sourceQuery : resolveTopicList(context)) {
            String readUri = buildOpcUaEndpointUri(context, sourceQuery, false);
            // MiloClientProducer enters the read path only when CamelMiloNodeIds is present;
            // body=null alone triggers a write (null value) rather than a read.
            @SuppressWarnings("unchecked")
            List<DataValue> readResult = (List<DataValue>) producer.requestBodyAndHeaders(
                readUri, null,
                Map.of(MiloConstants.HEADER_NODE_IDS, List.of(sourceQuery),
                       MiloConstants.HEADER_AWAIT, true));
            if (readResult != null && !readResult.isEmpty()) {
                normalizedRows.addAll(normalizeInboundPayload(context, sourceQuery, readResult.get(0)));
            }
            readUriList.add(readUri);
        }

        if (!normalizedRows.isEmpty()) {
            producer.sendBody("direct:opcua-read-device-request", normalizedRows);
        }

        return Map.of(
            "status", "completed",
            "routeId", "opcua-read-device-request",
            "rowCount", normalizedRows.size(),
            "readUriList", readUriList
        );
    }

    public Map<String, Object> runMqttWrite(RequestContext context) {
        assertRequestType(context, "DrtWrite", "DrtConfigWrite");
        if (context.brokerUri() == null || context.brokerUri().isBlank()) {
            throw new IllegalArgumentException("DeviceRequest " + context.requestName() + " does not define brokerUri.");
        }

        List<String> publishUriList = new ArrayList<>();
        for (RequestItem item : context.items()) {
            String topic = requireQuery(context, item);
            String publishUri = buildEndpointUri(context.brokerUri(), topic);
            producer.sendBody(publishUri, toJson(buildMqttPayload(item)));
            publishUriList.add(publishUri);
        }

        if (mqttWriteAfterPublishEnabled) {
            producer.sendBody(mqttWriteAfterPublishUri, Map.of("requestName", context.requestName()));
        }

        return Map.of(
            "status", "completed",
            "routeId", "mqtt-write-device-request",
            "rowCount", context.items().size(),
            "publishUriList", publishUriList
        );
    }

    public Map<String, Object> runOpcUaWrite(RequestContext context) {
        assertRequestType(context, "DrtWrite", "DrtConfigWrite");
        assertOpcUa(context);

        int timeoutMs = context.timeout() != null ? context.timeout() * 1000 : OPCUA_WRITE_DEFAULT_TIMEOUT_MS;
        List<String> publishUriList = new ArrayList<>();
        for (RequestItem item : context.items()) {
            String nodeId = requireQuery(context, item);
            String publishUri = buildOpcUaEndpointUri(context, nodeId, false);
            writeWithRetry(publishUri, resolveOutboundValue(item), timeoutMs);
            publishUriList.add(publishUri);
        }

        if (opcuaWriteAfterPublishEnabled) {
            producer.sendBody(opcuaWriteAfterPublishUri, Map.of("requestName", context.requestName()));
        }

        return Map.of(
            "status", "completed",
            "routeId", "opcua-write-device-request",
            "rowCount", context.items().size(),
            "publishUriList", publishUriList
        );
    }

    private void writeWithRetry(String publishUri, Object value, int timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        StatusCode lastStatus = null;
        Exception lastException = null;
        int attempts = 0;
        do {
            attempts++;
            try {
                Object result = producer.requestBodyAndHeader(publishUri, value, MiloConstants.HEADER_AWAIT, true);
                if (result instanceof StatusCode sc && sc.isGood()) return;
                lastStatus = result instanceof StatusCode sc ? sc : null;
                lastException = null;
            } catch (Exception e) {
                lastException = e;
                lastStatus = null;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) break;
            try { Thread.sleep(Math.min(OPCUA_WRITE_RETRY_DELAY_MS, remaining)); }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("OPC UA write to " + publishUri + " interrupted", ie);
            }
        } while (System.currentTimeMillis() < deadline);
        String detail = lastException != null ? lastException.getMessage()
            : lastStatus != null ? lastStatus.toString() : "timeout";
        logger.warnf("OPC UA write to %s failed after %d attempt(s) in %dms: %s",
            publishUri, attempts, timeoutMs, detail);
        throw new IllegalStateException("OPC UA write to " + publishUri + " failed: " + detail, lastException);
    }

    /**
     * Registers a dynamic MQTT consumer that forwards raw LoggerFacade batches
     * (CODESYS numbered-object format) directly to {@code direct:plc-log-ingest}.
     * Used for DeviceRequests with {@code purposeEnumId = 'DrpLogging'}.
     * The DeviceRequest.query field is the MQTT topic; no items are required.
     */
    public Map<String, Object> subscribePlcLog(RequestContext context) {
        assertSubscribeRequest(context);
        if (context.brokerUri() == null || context.brokerUri().isBlank()) {
            throw new IllegalArgumentException("DeviceRequest " + context.requestName() + " does not define brokerUri.");
        }
        List<String> topicList = resolvePlcLogTopicList(context);
        if (topicList.isEmpty()) {
            throw new IllegalArgumentException("DeviceRequest " + context.requestName() + " has no topic (set the QUERY field to the MQTT topic, e.g. 'moqui-plc').");
        }

        List<String> routeIdList = new ArrayList<>();
        List<String> consumerUriList = new ArrayList<>();
        for (int index = 0; index < topicList.size(); index++) {
            String topic = topicList.get(index);
            String routeId = routePrefix(context.requestName()) + index;
            String consumerUri = buildMqttConsumerUri(context.brokerUri(), topic, configuredGatewayDeviceId(), context.requestName(), index);

            Object lock = subscriptionLocks.computeIfAbsent(routeId, k -> new Object());
            synchronized (lock) {
                if (camelContext.getRoute(routeId) == null) {
                    addPlcLogSubscriptionRoute(routeId, consumerUri, context);
                }
            }

            if (camelContext.getRoute(routeId) != null) {
                routeIdList.add(routeId);
                consumerUriList.add(consumerUri);
            }
        }

        assertAtLeastOneRouteStarted(context.requestName(), routeIdList);
        return Map.of(
            "status", "completed",
            "routeId", "plc-log-subscribe-device-request",
            "routeIdList", routeIdList,
            "consumerUriList", consumerUriList
        );
    }

    public Map<String, Object> subscribeMqtt(RequestContext context) {
        assertSubscribeRequest(context);
        if (context.brokerUri() == null || context.brokerUri().isBlank()) {
            throw new IllegalArgumentException("DeviceRequest " + context.requestName() + " does not define brokerUri.");
        }
        warnIfStaticMqttConsumerActive();
        return registerDynamicRoutes(context, false);
    }

    public Map<String, Object> subscribeOpcUa(RequestContext context) {
        assertSubscribeRequest(context);
        assertOpcUa(context);
        return registerDynamicRoutes(context, true);
    }

    public Map<String, Object> unsubscribe(RequestContext context) {
        String targetRequestName = (context.parentRequestName() != null && !context.parentRequestName().isBlank())
            ? context.parentRequestName()
            : context.requestName();
        String prefix = routePrefix(targetRequestName);
        List<String> routeIdList = camelContext.getRoutes().stream()
            .map(route -> route.getId())
            .filter(routeId -> routeId.startsWith(prefix))
            .toList();

        List<String> failures = new ArrayList<>();
        for (String routeId : routeIdList) {
            try {
                camelContext.getRouteController().stopRoute(routeId);
                camelContext.removeRoute(routeId);
                subscriptionLocks.remove(routeId);
            } catch (Exception e) {
                logger.warnf(e, "Failed to stop subscription route %s for request %s.", routeId, context.requestName());
                failures.add(routeId + ": " + e.getMessage());
            }
        }

        if (!failures.isEmpty()) {
            throw new IllegalStateException("Unsubscribe for " + context.requestName() + " failed to stop "
                + failures.size() + " route(s): " + String.join("; ", failures));
        }

        return Map.of(
            "status", "completed",
            "routeId", "rest-unsubscribe-device-request",
            "routeIdList", routeIdList,
            "targetRequestName", targetRequestName
        );
    }

    public RequestContext loadRequestContext(String requestName) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement requestPs = connection.prepareStatement(requestSql);
             PreparedStatement itemsPs = connection.prepareStatement(requestItemsSql)) {

            requestPs.setString(1, requestName);
        String reqName, deviceId, parentReqName, reqType, purposeId, routerId, connName,
                   brokerUri, queryStr, onlyChanged, driverEnumId, driverEnumCode,
                   transportEnumId, transportEnumCode, transportConfig, options;
            Integer timeout, pollingInterval;

            try (ResultSet rs = requestPs.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalArgumentException("DeviceRequest with name " + requestName + " not found.");
                }
                reqName = rs.getString("REQUEST_NAME");
                deviceId = rs.getString("DEVICE_ID");
                parentReqName = rs.getString("PARENT_REQUEST_NAME");
                reqType = rs.getString("REQUEST_TYPE_ENUM_ID");
                purposeId = rs.getString("PURPOSE_ENUM_ID");
                routerId = rs.getString("ROUTER_ENUM_ID");
                connName = rs.getString("CONNECTION_NAME");
                brokerUri = rs.getString("BROKER_URI");
                timeout = rs.getObject("TIMEOUT") != null ? rs.getInt("TIMEOUT") : null;
                pollingInterval = rs.getObject("POLLING_INTERVAL") != null ? rs.getInt("POLLING_INTERVAL") : null;
                queryStr = rs.getString("QUERY");
                onlyChanged = rs.getString("ONLY_CHANGED_PARAMETERS");
                driverEnumId = rs.getString("DRIVER_ENUM_ID");
                driverEnumCode = rs.getString("DRIVER_ENUM_CODE");
                transportEnumId = rs.getString("TRANSPORT_ENUM_ID");
                transportEnumCode = rs.getString("TRANSPORT_ENUM_CODE");
                transportConfig = rs.getString("TRANSPORT_CONFIG");
                options = rs.getString("OPTIONS");
            }

            itemsPs.setString(1, requestName);
            List<RequestItem> items = new ArrayList<>();
            try (ResultSet rs = itemsPs.executeQuery()) {
                while (rs.next()) {
                    items.add(new RequestItem(
                        rs.getString("PARAMETER_ID"),
                        rs.getObject("SEQUENCE_NUM") != null ? rs.getInt("SEQUENCE_NUM") : null,
                        rs.getObject("NUMERIC_VALUE"),
                        rs.getString("SYMBOLIC_VALUE"),
                        rs.getString("PARAMETER_ENUM_ID"),
                        rs.getString("TARGET_QUERY")
                    ));
                }
            }

            return new RequestContext(
                reqName, deviceId, parentReqName, reqType, purposeId, routerId, connName,
                brokerUri, timeout, pollingInterval, queryStr, onlyChanged,
                driverEnumId, driverEnumCode, transportEnumId, transportEnumCode,
                transportConfig, options, List.copyOf(items)
            );
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to resolve DeviceRequest " + requestName + ": " + e.getMessage(), e);
        }
    }

    public List<Map<String, Object>> coerceInboundRows(Object body) {
        if (body instanceof List<?> bodyList) {
            List<Map<String, Object>> rows = new ArrayList<>(bodyList.size());
            for (Object row : bodyList) rows.add(castMap((Map<?, ?>) row));
            return rows;
        }
        if (body instanceof Map<?, ?> row) return List.of(castMap(row));
        throw new IllegalArgumentException("Inbound payload must be a Map or List of Map rows.");
    }

    public List<Map<String, Object>> toParameterUpsertRows(List<Map<String, Object>> rows) {
        List<Map<String, Object>> upsertRows = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            Map<String, Object> upsertRow = new LinkedHashMap<>();
            upsertRow.put("parameterId", row.get("parameterId"));
            upsertRow.put("numericValue", row.get("numericValue"));
            upsertRow.put("symbolicValue", row.get("symbolicValue"));
            upsertRow.put("parameterEnumId", row.get("parameterEnumId"));
            upsertRows.add(upsertRow);
        }
        return upsertRows;
    }

    public List<Map<String, Object>> toParameterLogRows(List<Map<String, Object>> rows) {
        List<Map<String, Object>> logRows = new ArrayList<>(rows.size());
        long baseSequence = System.currentTimeMillis();
        for (int index = 0; index < rows.size(); index++) {
            Map<String, Object> row = rows.get(index);
            Map<String, Object> logRow = new LinkedHashMap<>();
            logRow.put("parameterLogId", row.getOrDefault("parameterLogId", java.util.UUID.randomUUID().toString()));
            logRow.put("sequenceNum", row.getOrDefault("sequenceNum", baseSequence + index));
            logRow.put("parameterId", row.get("parameterId"));
            logRow.put("observedDate", row.get("observedDate"));
            logRow.put("numericValue", row.get("numericValue"));
            logRow.put("symbolicValue", row.get("symbolicValue"));
            logRow.put("parameterEnumId", row.get("parameterEnumId"));
            logRows.add(logRow);
        }
        return logRows;
    }

    public PlcLogBatch partitionPlcLogBatch(Object body) {
        SimpleDateFormat sdf = new SimpleDateFormat("'DT#'yyyy-MM-dd-HH:mm:ss");
        Map<?, ?> batchMap = body instanceof Map<?, ?> map ? map : Map.of();
        List<Map<String, Object>> withSource = new ArrayList<>();
        List<Map<String, Object>> withoutSource = new ArrayList<>();

        int index = 0;
        for (Object entryObject : batchMap.values()) {
            if (!(entryObject instanceof Map<?, ?> entryMapRaw)) continue;
            Map<String, Object> entry = castMap(entryMapRaw);
            Timestamp observedDate = parsePlcTimestamp(sdf, Objects.toString(entry.get("logEventDate"), ""));
            String loggerName = Objects.toString(entry.getOrDefault("loggerName", "plc-log-unknown"));
            String source = Objects.toString(entry.getOrDefault("source", ""));
            Integer type = entry.get("type") instanceof Number n ? n.intValue() : null;

            if (!source.isBlank()) {
                Map<String, Object> parameterRow = new LinkedHashMap<>();
                parameterRow.put("parameterId", loggerName + "." + source);
                parameterRow.put("observedDate", observedDate);
                parameterRow.put("numericValue", Integer.valueOf(1).equals(type) ? entry.get("numericValue") : null);
                parameterRow.put("symbolicValue", Integer.valueOf(2).equals(type)
                    ? Objects.toString(entry.get("enumValue"), null)
                    : Objects.toString(entry.get("message"), null));
                parameterRow.put("purposeEnumId", "DrpLogging");
                withSource.add(parameterRow);
            } else {
                Map<String, Object> deviceRow = new LinkedHashMap<>();
                deviceRow.put("deviceLogId", java.util.UUID.randomUUID().toString());
                deviceRow.put("deviceId", loggerName);
                deviceRow.put("payload", toJson(entry));
                Object rawSequence = entry.get("sequenceNum");
                Integer sequenceNum = rawSequence instanceof Number number ? number.intValue() : index + 1;
                deviceRow.put("sequenceNum", sequenceNum);
                deviceRow.put("observedDate", observedDate);
                withoutSource.add(deviceRow);
            }
            index++;
        }

        return new PlcLogBatch(List.copyOf(withSource), List.copyOf(withoutSource));
    }

    public List<Map<String, Object>> uniqueParameterRefs(List<Map<String, Object>> rows) {
        LinkedHashMap<String, Map<String, Object>> uniqueRows = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            String parameterId = Objects.toString(row.get("parameterId"), null);
            if (parameterId != null && !parameterId.isBlank()) {
                uniqueRows.putIfAbsent(parameterId, Map.of("parameterId", parameterId));
            }
        }
        return new ArrayList<>(uniqueRows.values());
    }

    public List<Map<String, Object>> unwrapSqlRowList(Object body) {
        if (body instanceof Map<?, ?> map && map.containsKey("rowList")) {
            Object rowList = map.get("rowList");
            if (rowList instanceof List<?> rows) return rows.stream().map(row -> castMap((Map<?, ?>) row)).toList();
        }
        if (body instanceof List<?> rows) return rows.stream().map(row -> castMap((Map<?, ?>) row)).toList();
        if (body instanceof Map<?, ?> map) return List.of(castMap(map));
        return List.of();
    }

    public List<Map<String, Object>> buildExportFileList(List<Map<String, Object>> rows) {
        String ruleSetName = rows.isEmpty() ? "recipe" : Objects.toString(rows.get(0).getOrDefault("ruleSetName", "recipe"));
        Map<Integer, List<Map<String, Object>>> byPriority = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            int priority = row.get("priority") instanceof Number n ? n.intValue() : 0;
            byPriority.computeIfAbsent(priority, ignored -> new ArrayList<>()).add(row);
        }

        return byPriority.entrySet().stream()
            .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
            .map(entry -> {
                int priority = entry.getKey();
                List<Map<String, Object>> group = entry.getValue();
                String content = group.stream()
                    .map(row -> Objects.toString(row.get("parameterName"), "")
                        + ":=" + Objects.toString(row.get("parameterValue"), ""))
                    .reduce((left, right) -> left + "\n" + right)
                    .orElse("");
                Map<String, Object> fileSpec = new LinkedHashMap<>();
                fileSpec.put("filename", "%s_p%02d.txt".formatted(ruleSetName, priority));
                fileSpec.put("content", content);
                fileSpec.put("priority", priority);
                fileSpec.put("rowCount", group.size());
                return fileSpec;
            })
            .toList();
    }

    public Map<String, Object> exportDeviceConfig(Map<String, Object> params) {
        Map<String, Object> queryParams = new LinkedHashMap<>();
        queryParams.put("deviceRuleSetId", params.get("deviceRuleSetId"));
        queryParams.put("deviceId", params.get("deviceId"));
        queryParams.put("deviceRuleId", params.get("deviceRuleId"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = producer.requestBody(deviceConfigExportFetchUri, queryParams, List.class);
        List<Map<String, Object>> fileList = buildExportFileList(unwrapSqlRowList(rows));
        for (Map<String, Object> fileSpec : fileList) {
            producer.sendBodyAndHeader("direct:transfer-file", fileSpec.get("content"), "CamelFileName", fileSpec.get("filename"));
        }
        return Map.of(
            "status", "completed",
            "routeId", "run-device-config-export",
            "files", fileList.stream()
                .map(file -> Map.of(
                    "filename", file.get("filename"),
                    "priority", file.get("priority"),
                    "rowCount", file.get("rowCount")))
                .toList()
        );
    }

    private Map<String, Object> registerDynamicRoutes(RequestContext context, boolean opcua) {
        List<String> topicList = resolveTopicList(context);
        if (topicList.isEmpty()) {
            throw new IllegalArgumentException("DeviceRequest " + context.requestName() + " has no topic/address to subscribe.");
        }

        List<String> routeIdList = new ArrayList<>();
        List<String> consumerUriList = new ArrayList<>();
        for (int index = 0; index < topicList.size(); index++) {
            String sourceQuery = topicList.get(index);
            String routeId = routePrefix(context.requestName()) + index;
            String consumerUri = opcua
                ? buildOpcUaEndpointUri(context, sourceQuery, true)
                : buildMqttConsumerUri(context.brokerUri(), sourceQuery, configuredGatewayDeviceId(), context.requestName(), index);

            Object lock = subscriptionLocks.computeIfAbsent(routeId, k -> new Object());
            synchronized (lock) {
                if (camelContext.getRoute(routeId) == null) {
                    addSubscriptionRoute(routeId, consumerUri, context, sourceQuery, opcua);
                }
            }

            if (camelContext.getRoute(routeId) != null) {
                routeIdList.add(routeId);
                consumerUriList.add(consumerUri);
            }
        }

        assertAtLeastOneRouteStarted(context.requestName(), routeIdList);

        return Map.of(
            "status", "completed",
            "routeId", opcua ? "opcua-subscribe-device-request" : "mqtt-subscribe-device-request",
            "routeIdList", routeIdList,
            "consumerUriList", consumerUriList
        );
    }

    private void addSubscriptionRoute(String routeId, String consumerUri, RequestContext context, String sourceQuery, boolean opcua) {
        try {
            camelContext.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from(consumerUri).routeId(routeId)
                        .process(exchange -> {
                            exchange.setProperty("gateway.routeId", routeId);
                            exchange.setProperty("gateway.requestName", context.requestName());
                            try {
                                List<Map<String, Object>> normalized =
                                    normalizeInboundPayload(context, sourceQuery, exchange.getIn().getBody());
                                exchange.getIn().setBody(normalized);
                                exchange.setProperty("gateway.skipDispatch", normalized.isEmpty());
                            } catch (Exception e) {
                                exchange.setProperty("gateway.skipDispatch", true);
                                logger.warnf(e,
                                    "Discarding inbound %s payload for request %s on %s after normalization failure.",
                                    opcua ? "OPC UA" : "MQTT", context.requestName(), sourceQuery);
                            }
                        })
                        .filter().simple("${exchangeProperty.gateway.skipDispatch} != true")
                        .to(opcua ? "direct:opcua-read-device-request" : "direct:mqtt-read-device-request");
                }
            });
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to register subscription route " + routeId
                    + " for DeviceRequest " + context.requestName(),
                e);
        }
    }

    private List<Map<String, Object>> normalizeInboundPayload(RequestContext context, String sourceQuery, Object payload) {
        Object decodedPayload = decodeIncomingPayload(payload);
        if (decodedPayload == null) return List.of();

        if (decodedPayload instanceof List<?> payloadList) {
            List<Map<String, Object>> rowList = new ArrayList<>();
            for (int i = 0; i < payloadList.size(); i++) {
                Object row = payloadList.get(i);
                if (row instanceof Map<?, ?> rowMap) {
                    rowList.add(normalizeInboundRow(context, sourceQuery, castMap(rowMap), i));
                } else {
                    Map<String, Object> normalized = normalizeScalarValue(context, sourceQuery, row, i);
                    if (normalized != null) rowList.add(normalized);
                }
            }
            return rowList;
        }

        if (decodedPayload instanceof Map<?, ?> rowMap) {
            return List.of(normalizeInboundRow(context, sourceQuery, castMap(rowMap), 0));
        }

        Map<String, Object> normalized = normalizeScalarValue(context, sourceQuery, decodedPayload, 0);
        return normalized != null ? List.of(normalized) : List.of();
    }

    private Map<String, Object> normalizeInboundRow(RequestContext context, String sourceQuery, Map<String, Object> row, int index) {
        Map<String, Object> normalized = new LinkedHashMap<>(row);
        if (!normalized.containsKey("parameterId") || normalized.get("parameterId") == null) {
            RequestItem item = resolveRequestItem(context, sourceQuery, index);
            if (item != null) normalized.put("parameterId", item.parameterId());
        }
        normalized.putIfAbsent("purposeEnumId", context.purposeEnumId());
        normalized.putIfAbsent("observedDate", Timestamp.from(Instant.now()));
        return normalized;
    }

    private Map<String, Object> normalizeScalarValue(RequestContext context, String sourceQuery, Object value, int index) {
        RequestItem item = resolveRequestItem(context, sourceQuery, index);
        if (item == null) return null;

        Map<String, Object> normalized = new LinkedHashMap<>();
        normalized.put("parameterId", item.parameterId());
        normalized.put("purposeEnumId", context.purposeEnumId());
        normalized.put("observedDate", Timestamp.from(Instant.now()));

        if (value instanceof Number number) {
            normalized.put("numericValue", number);
        } else if (value instanceof Boolean bool) {
            normalized.put("symbolicValue", bool ? "Y" : "N");
        } else if (value != null) {
            normalized.put("symbolicValue", value.toString());
        }

        return normalized;
    }

    private Object decodeIncomingPayload(Object payload) {
        if (payload == null) return null;
        if (payload instanceof DataValue dataValue) return decodeIncomingPayload(dataValue.getValue());
        if (payload instanceof Variant variant) return decodeIncomingPayload(variant.getValue());
        if (payload instanceof byte[] bytes) return decodeIncomingPayload(new String(bytes, StandardCharsets.UTF_8));
        if (payload instanceof String stringPayload) return parseStringPayload(stringPayload);
        return payload;
    }

    private Object parseStringPayload(String payload) {
        String trimmed = payload.trim();
        if (trimmed.isEmpty()) return null;
        try {
            if (trimmed.startsWith("[")) return objectMapper.readValue(trimmed, LIST_OBJECT);
            if (trimmed.startsWith("{")) return objectMapper.readValue(trimmed, MAP_OBJECT);
        } catch (JsonProcessingException e) {
            return trimmed;
        }
        return trimmed;
    }

    private RequestItem resolveRequestItem(RequestContext context, String sourceQuery, int index) {
        if (Objects.equals(context.requestQuery(), sourceQuery)) {
            return index < context.items().size() ? context.items().get(index) : null;
        }
        return context.items().stream()
            .filter(item -> Objects.equals(item.targetQuery(), sourceQuery))
            .findFirst()
            .orElse(null);
    }

    private List<String> resolveTopicList(RequestContext context) {
        Set<String> topicSet = new LinkedHashSet<>();
        if (context.requestQuery() != null && !context.requestQuery().isBlank() && !context.items().isEmpty()) {
            topicSet.add(context.requestQuery());
        }
        for (RequestItem item : context.items()) {
            if (item.targetQuery() != null && !item.targetQuery().isBlank()) topicSet.add(item.targetQuery());
        }
        return new ArrayList<>(topicSet);
    }

    private String buildEndpointUri(String baseUri, String query) {
        if (baseUri == null || baseUri.isBlank()) return baseUri;
        if (query == null || query.isBlank()) return baseUri;
        int queryIndex = baseUri.indexOf('?');
        return queryIndex >= 0
            ? baseUri.substring(0, queryIndex) + query + baseUri.substring(queryIndex)
            : baseUri + query;
    }

    /**
     * Dynamic MQTT subscriptions must reconnect automatically after temporary broker loss.
     * We enforce conservative consumer defaults here instead of relying on seed data to
     * remember component-specific reconnect flags.
     */
    public String buildMqttConsumerUri(String baseUri, String query, String gatewayDeviceId, String requestName, int index) {
        String endpointUri = buildEndpointUri(baseUri, query);
        if (endpointUri == null || endpointUri.isBlank() || !endpointUri.startsWith("paho-mqtt5:")) {
            return endpointUri;
        }

        StringBuilder sb = new StringBuilder(endpointUri);
        String separator = endpointUri.contains("?") ? "&" : "?";
        if (!endpointUri.contains("clientId=")) {
            String clientId = "moqui-gw-"
                + safeClientIdPart(gatewayDeviceId)
                + "-"
                + safeClientIdPart(requestName)
                + "-"
                + index;
            sb.append(separator).append("clientId=").append(clientId);
            separator = "&";
        }
        if (!endpointUri.contains("automaticReconnect=")) {
            sb.append(separator).append("automaticReconnect=true");
            separator = "&";
        }
        if (!endpointUri.contains("cleanStart=")) {
            sb.append(separator).append("cleanStart=false");
            separator = "&";
        }
        if (!endpointUri.contains("sessionExpiryInterval=")) {
            sb.append(separator).append("sessionExpiryInterval=86400");
            separator = "&";
        }
        if (!endpointUri.contains("maxReconnectDelay=")) {
            sb.append(separator).append("maxReconnectDelay=5000");
        }
        return sb.toString();
    }

    private String safeClientIdPart(String value) {
        return value == null ? "unknown" : value.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private String buildOpcUaEndpointUri(RequestContext context, String nodeId, boolean consumer) {
        String endpointUri = buildOpcUaEndpointBase(context);
        StringBuilder sb = new StringBuilder(endpointUri);
        char separator = endpointUri.contains("?") ? '&' : '?';
        sb.append(separator).append("node=RAW(").append(nodeId).append(')');
        sb.append("&clientId=").append(context.requestName());
        if (consumer) {
            sb.append("&bridgeErrorHandler=true");
            if (context.pollingInterval() != null && context.pollingInterval() > 0) {
                sb.append("&samplingInterval=").append(context.pollingInterval());
            }
        }
        return sb.toString();
    }

    private String buildOpcUaEndpointBase(RequestContext context) {
        if ((context.connectionName() == null || context.connectionName().isBlank())
            && context.brokerUri() != null && !context.brokerUri().isBlank()) {
            if (context.brokerUri().startsWith("milo-client:")) return context.brokerUri();
            if (context.brokerUri().startsWith("opc.tcp://")) return "milo-client:" + context.brokerUri();
            if (context.brokerUri().startsWith("opcua:")) return "milo-client:" + context.brokerUri().substring("opcua:".length());
        }

        if (context.transportConfig() == null || context.transportConfig().isBlank()) {
            throw new IllegalArgumentException("OPC UA DeviceConnection " + context.connectionName() + " does not define transportConfig.");
        }

        String transportScheme = "opc.tcp";
        if (context.transportEnumCode() != null && !context.transportEnumCode().isBlank() && !"tcp".equals(context.transportEnumCode())) {
            transportScheme = "opc." + context.transportEnumCode();
        }

        String endpointPath = context.transportConfig().startsWith("opc.")
            ? context.transportConfig()
            : transportScheme + "://" + context.transportConfig();
        return "milo-client:" + endpointPath + appendOptions(context.options());
    }

    private String appendOptions(String options) {
        if (options == null || options.isBlank()) return "";
        return options.startsWith("?") ? options : "?" + options;
    }

    private Object resolveOutboundValue(RequestItem item) {
        if (item.numericValue() instanceof Number n) return n.doubleValue();
        if (item.parameterEnumId() != null && !item.parameterEnumId().isBlank()) return item.parameterEnumId();
        return item.symbolicValue();
    }

    private Map<String, Object> buildMqttPayload(RequestItem item) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("parameterId", item.parameterId());
        if (item.numericValue() != null) payload.put("numericValue", item.numericValue());
        if (item.symbolicValue() != null) payload.put("symbolicValue", item.symbolicValue());
        if (item.parameterEnumId() != null) payload.put("parameterEnumId", item.parameterEnumId());
        return payload;
    }

    private String toJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize outbound payload: " + e.getMessage(), e);
        }
    }

    private String requireQuery(RequestContext context, RequestItem item) {
        String query = item.targetQuery() != null && !item.targetQuery().isBlank() ? item.targetQuery() : context.requestQuery();
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Missing query/node definition for request " + context.requestName()
                + " item " + item.parameterId() + ".");
        }
        return query;
    }

    private void assertRequestType(RequestContext context, String... allowed) {
        for (String value : allowed) {
            if (value.equals(context.requestTypeEnumId())) return;
        }
        throw new IllegalArgumentException("Unsupported request type " + context.requestTypeEnumId()
            + " for request " + context.requestName() + ".");
    }

    private void assertSubscribeRequest(RequestContext context) {
        if (!List.of("DrtSubscribe", "DrtEvent", "DrtStateChange", "DrtCyclic").contains(context.requestTypeEnumId())) {
            throw new IllegalArgumentException("DeviceRequest " + context.requestName() + " is not a subscribe request.");
        }
    }

    private void assertOpcUa(RequestContext context) {
        if (!isOpcuaContext(context)) {
            throw new IllegalArgumentException("DeviceRequest " + context.requestName()
                + " is not bound to an OPC UA direct connection.");
        }
    }

    private boolean isOpcuaContext(RequestContext context) {
        if ("DcdOpcUa".equals(context.driverEnumId())) return true;
        return context.brokerUri() != null
            && (context.brokerUri().startsWith("opcua:")
                || context.brokerUri().startsWith("milo-client:")
                || context.brokerUri().startsWith("opc.tcp://"));
    }

    /**
     * A DeviceRequest is a PLC log request when its purposeEnumId is 'DrpLogging'.
     * These requests use the LoggerFacade batch format and are routed to plc-log-ingest
     * rather than the standard parameter normalization pipeline.
     */
    private boolean isPlcLogContext(RequestContext context) {
        return "DrpLogging".equals(context.purposeEnumId());
    }

    private boolean isSubscribeRequestType(String requestType) {
        return List.of("DrtSubscribe", "DrtEvent", "DrtStateChange", "DrtCyclic").contains(requestType);
    }

    private Timestamp parsePlcTimestamp(SimpleDateFormat sdf, String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return new Timestamp(sdf.parse(value).getTime());
        } catch (ParseException ignored) {
            return null;
        }
    }

    /**
     * For PLC log subscriptions the topic comes solely from requestQuery —
     * DeviceRequestItems are not used (the LoggerFacade batch is self-describing).
     */
    private List<String> resolvePlcLogTopicList(RequestContext context) {
        if (context.requestQuery() != null && !context.requestQuery().isBlank()) {
            return List.of(context.requestQuery());
        }
        return List.of();
    }

    /**
     * Adds a dynamic MQTT consumer route that converts the raw bytes to a String,
     * parses the JSON LoggerFacade batch, and forwards it to direct:plc-log-ingest.
     * Normalization is intentionally skipped — plc-log-ingest handles the batch format.
     */
    private void addPlcLogSubscriptionRoute(String routeId, String consumerUri, RequestContext context) {
        try {
            camelContext.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from(consumerUri).routeId(routeId)
                        .doTry()
                            .convertBodyTo(String.class)
                            .unmarshal().json()
                            .setProperty("gateway.routeId").constant(routeId)
                            .setProperty("gateway.requestName").constant(context.requestName())
                            .to("direct:plc-log-ingest")
                        .doCatch(Exception.class)
                            .log(org.apache.camel.LoggingLevel.WARN,
                                "Discarding PLC log payload for request " + context.requestName()
                                + " after runtime failure: ${exception.message}")
                        .end();
                }
            });
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to register PLC log subscription route " + routeId
                    + " for DeviceRequest " + context.requestName(),
                e);
        }
    }

    private void warnIfStaticMqttConsumerActive() {
        try {
            var route = camelContext.getRoute("mqtt-read-device-request-consumer");
            if (route == null) return;
            var status = camelContext.getRouteController().getRouteStatus("mqtt-read-device-request-consumer");
            if (status != null && status.isStarted() && !route.getEndpoint().getEndpointUri().startsWith("seda:")) {
                logger.warn("Static MQTT consumer (mqtt-read-device-request-consumer) is running on a real broker. " +
                    "Dynamic subscriptions and the static consumer should not listen on overlapping topics. " +
                    "Set mqtt.read.route.autoStartup=false to use dynamic subscriptions exclusively.");
            }
        } catch (Exception e) {
            logger.debugf(e, "Unable to inspect static MQTT consumer state");
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Map<?, ?> map) {
        return (Map<String, Object>) map;
    }

    private String routePrefix(String requestName) {
        return "device-request-consumer-" + requestName + "-";
    }

    public GatewayIdentityStatus inspectGatewayIdentity() {
        String gatewayDeviceId = configuredGatewayDeviceId();
        if (gatewayDeviceId == null || gatewayDeviceId.isBlank()) {
            return new GatewayIdentityStatus(gatewayDeviceId, false, false, false,
                "gateway.device.id / GATEWAY_DEVICE_ID is required.");
        }

        boolean physicalDeviceExists;
        boolean gatewayMemberFound;
        try (Connection connection = dataSource.getConnection()) {
            physicalDeviceExists = queryExists(connection, GATEWAY_DEVICE_EXISTS_SQL, gatewayDeviceId);
            gatewayMemberFound = physicalDeviceExists
                && queryCount(connection, GATEWAY_MEMBERSHIP_COUNT_SQL, gatewayDeviceId) > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to validate gateway identity " + gatewayDeviceId, e);
        }

        if (!physicalDeviceExists) {
            return new GatewayIdentityStatus(gatewayDeviceId, true, false, false,
                "Configured gateway.device.id " + gatewayDeviceId + " does not exist as Device + PhysicalDevice.");
        }
        if (!gatewayMemberFound) {
            return new GatewayIdentityStatus(gatewayDeviceId, true, true, false,
                "Gateway " + gatewayDeviceId + " is not assigned to any DeviceGroup as DgmpEdgeGateway.");
        }
        return new GatewayIdentityStatus(gatewayDeviceId, true, true, true, "ready");
    }

    public void assertGatewayScope(RequestContext context) {
        if (context == null) {
            throw new IllegalArgumentException("RequestContext is required.");
        }
        if (context.deviceId() == null || context.deviceId().isBlank()) {
            return;
        }

        String gatewayDeviceId = configuredGatewayDeviceId();
        if (gatewayDeviceId.isBlank()) {
            throw new IllegalStateException("gateway.device.id / GATEWAY_DEVICE_ID is required.");
        }

        try (Connection connection = dataSource.getConnection()) {
            int visibleDeviceCount = queryCount(connection, GATEWAY_DEVICE_SCOPE_COUNT_SQL, gatewayDeviceId, context.deviceId());
            if (visibleDeviceCount <= 0) {
                throw new SecurityException(
                    "DeviceRequest " + context.requestName()
                        + " belongs to device " + context.deviceId()
                        + ", which is not in the scope of gateway " + gatewayDeviceId + "."
                );
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                "Unable to validate gateway scope for request " + context.requestName(),
                e
            );
        }
    }

    private String configuredGatewayDeviceId() {
        return gatewayDeviceId.orElse("");
    }

    public List<String> loadStartupSubscriptionRequestNames(String gatewayDeviceId) {
        if (gatewayDeviceId == null || gatewayDeviceId.isBlank()) {
            throw new IllegalStateException("gateway.device.id / GATEWAY_DEVICE_ID is required.");
        }

        List<String> requestNames = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(STARTUP_SUBSCRIPTION_SQL)) {
            ps.setString(1, gatewayDeviceId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) requestNames.add(rs.getString("REQUEST_NAME"));
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                "Unable to load startup DeviceRequest subscriptions for gatewayDeviceId " + gatewayDeviceId,
                e
            );
        }
        return requestNames;
    }

    private boolean queryExists(Connection connection, String sql, String value) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, value);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private int queryCount(Connection connection, String sql, String value) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, value);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    private int queryCount(Connection connection, String sql, String value1, String value2) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, value1);
            ps.setString(2, value2);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    private int extractRouteCount(Map<String, Object> result) {
        Object routeIdList = result.get("routeIdList");
        return routeIdList instanceof List<?> list ? list.size() : 0;
    }

    private void assertAtLeastOneRouteStarted(String requestName, List<String> routeIdList) {
        if (routeIdList == null || routeIdList.isEmpty()) {
            throw new IllegalStateException(
                "No Camel subscription route was registered for DeviceRequest " + requestName
            );
        }

        List<String> notStarted = new ArrayList<>();
        for (String routeId : routeIdList) {
            try {
                var status = camelContext.getRouteController().getRouteStatus(routeId);
                if (status == null || !status.isStarted()) notStarted.add(routeId + "=" + status);
            } catch (Exception e) {
                notStarted.add(routeId + "=" + e.getMessage());
            }
        }

        if (!notStarted.isEmpty()) {
            throw new IllegalStateException(
                "Some Camel subscription routes are not started for DeviceRequest "
                    + requestName + ": " + String.join(", ", notStarted)
            );
        }
    }

    public record GatewayIdentityStatus(
        String gatewayDeviceId,
        boolean configured,
        boolean physicalDeviceExists,
        boolean gatewayMemberFound,
        String message
    ) {
        public boolean ready() {
            return configured && physicalDeviceExists && gatewayMemberFound;
        }
    }

    public record RequestContext(
        String requestName,
        String deviceId,
        String parentRequestName,
        String requestTypeEnumId,
        String purposeEnumId,
        String routerEnumId,
        String connectionName,
        String brokerUri,
        Integer timeout,
        Integer pollingInterval,
        String requestQuery,
        String onlyChangedParameters,
        String driverEnumId,
        String driverEnumCode,
        String transportEnumId,
        String transportEnumCode,
        String transportConfig,
        String options,
        List<RequestItem> items
    ) {}

    public record RequestItem(
        String parameterId,
        Integer sequenceNum,
        Object numericValue,
        String symbolicValue,
        String parameterEnumId,
        String targetQuery
    ) {}

    public record PlcLogBatch(
        List<Map<String, Object>> withSource,
        List<Map<String, Object>> withoutSource
    ) {}
}
