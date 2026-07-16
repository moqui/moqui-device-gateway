package org.moqui.device.gateway;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.moqui.device.gateway.service.GatewayRequestService;
import org.moqui.device.gateway.service.InboundErrorNotifier;
import org.jboss.logging.Logger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class GatewayRouteBuilder extends RouteBuilder {
    private static final Logger logger = Logger.getLogger(GatewayRouteBuilder.class);

    @Inject
    GatewayRequestService gatewayRequestService;

    @Inject
    InboundErrorNotifier inboundErrorNotifier;

    @Override
    public void configure() {
        from("direct:dispatch-device-request")
            .routeId("dispatch-device-request")
            .bean(gatewayRequestService, "dispatch");

        from("{{mqtt.read.consume.uri}}")
            .routeId("mqtt-read-device-request-consumer")
            .autoStartup("{{mqtt.read.route.autoStartup}}")
            .doTry()
                .convertBodyTo(String.class)
                .unmarshal().json(JsonLibrary.Jackson)
                .to("direct:mqtt-read-device-request")
            .doCatch(Exception.class)
                .log(LoggingLevel.WARN, "Discarding MQTT consumer payload after runtime failure: ${exception.message}")
            .end();

        from("{{opcua.read.consume.uri}}")
            .routeId("opcua-read-device-request-consumer")
            .autoStartup("{{opcua.read.route.autoStartup}}")
            .doTry()
                .convertBodyTo(String.class)
                .unmarshal().json(JsonLibrary.Jackson)
                .to("direct:opcua-read-device-request")
            .doCatch(Exception.class)
                .log(LoggingLevel.WARN, "Discarding OPC UA consumer payload after runtime failure: ${exception.message}")
            .end();

        from("direct:mqtt-read-device-request")
            .routeId("mqtt-read-device-request")
            .setProperty("protocolLabel").constant("MQTT")
            .setProperty("storeLogUri").simple("{{mqtt.read.store.log.uri}}")
            .setProperty("storeParameterUri").simple("{{mqtt.read.store.parameter.uri}}")
            .setProperty("afterStoreEnabled").simple("{{mqtt.read.afterStore.enabled}}")
            .setProperty("afterStoreUri").simple("{{mqtt.read.afterStore.uri}}")
            .to("direct:store-device-request-inbound");

        from("direct:opcua-read-device-request")
            .routeId("opcua-read-device-request")
            .setProperty("protocolLabel").constant("OPC UA")
            .setProperty("storeLogUri").simple("{{opcua.read.store.log.uri}}")
            .setProperty("storeParameterUri").simple("{{opcua.read.store.parameter.uri}}")
            .setProperty("afterStoreEnabled").simple("{{opcua.read.afterStore.enabled}}")
            .setProperty("afterStoreUri").simple("{{opcua.read.afterStore.uri}}")
            .to("direct:store-device-request-inbound");

        from("direct:store-device-request-inbound")
            .routeId("store-device-request-inbound")
            .process(this::prepareInboundRows)
            .choice()
                .when(simple("${exchangeProperty.purposeEnumId} != null && ${exchangeProperty.purposeEnumId} != 'DrpLogging'"))
                    .process(exchange -> exchange.getMessage().setBody(
                        gatewayRequestService.toParameterUpsertRows(currentRows(exchange))))
                    .to("direct:store-parameter-batch")
                .otherwise()
                    .process(exchange -> exchange.getMessage().setBody(
                        gatewayRequestService.toParameterLogRows(currentRows(exchange))))
                    .to("direct:store-log-batch")
            .end()
            .process(inboundErrorNotifier::clearError)
            .setBody(exchangeProperty("inboundPayload"))
            .choice()
                .when(simple("${exchangeProperty.afterStoreEnabled} == 'true'"))
                    .to("direct:after-store-callback")
            .end();

        from("direct:store-parameter-batch")
            .routeId("store-parameter-batch")
            .doTry()
                .toD("${exchangeProperty.storeParameterUri}")
            .doCatch(Exception.class)
                .log(LoggingLevel.WARN,
                    "Discarding ${exchangeProperty.protocolLabel} inbound payload after DB write failure: ${exception.message}")
                .process(inboundErrorNotifier::recordError)
                .stop()
            .end();

        from("direct:store-log-batch")
            .routeId("store-log-batch")
            .doTry()
                .toD("${exchangeProperty.storeLogUri}")
            .doCatch(Exception.class)
                .log(LoggingLevel.WARN,
                    "Discarding ${exchangeProperty.protocolLabel} inbound payload after DB write failure: ${exception.message}")
                .process(inboundErrorNotifier::recordError)
                .stop()
            .end();

        from("direct:after-store-callback")
            .routeId("after-store-callback")
            .setExchangePattern(ExchangePattern.InOnly)
            .doTry()
                .toD("${exchangeProperty.afterStoreUri}")
            .doCatch(Exception.class)
                .log(LoggingLevel.WARN,
                    "Ignoring ${exchangeProperty.protocolLabel} afterStore callback failure: ${exception.message}")
            .end();

        from("direct:run-device-config-export")
            .routeId("run-device-config-export")
            .bean(gatewayRequestService, "exportDeviceConfig");

        from("direct:transfer-file")
            .routeId("transfer-file")
            .setExchangePattern(ExchangePattern.InOnly)
            .toD("{{file.transfer.uri}}")
            .choice()
                .when(simple("${properties:file.transfer.uri.2.enabled:false}"))
                    .toD("{{file.transfer.uri.2}}")
            .end();

        from("direct:transfer-device-content")
            .routeId("transfer-device-content")
            .setExchangePattern(ExchangePattern.InOnly)
            .toD("${header.gatewayTransferUri}");

        from("{{plc.log.consume.uri}}")
            .routeId("plc-log-consumer")
            .autoStartup("{{plc.log.route.autoStartup}}")
            .doTry()
                .convertBodyTo(String.class)
                .unmarshal().json(JsonLibrary.Jackson)
                .to("direct:plc-log-ingest")
            .doCatch(Exception.class)
                .log(LoggingLevel.WARN, "Discarding PLC log payload after runtime failure: ${exception.message}")
            .end();

        from("direct:plc-log-ingest")
            .routeId("plc-log-ingest")
            .process(exchange -> {
                try {
                    GatewayRequestService.PlcLogBatch batch =
                        gatewayRequestService.partitionPlcLogBatch(exchange.getMessage().getBody());
                    exchange.setProperty("plcLogWithSource", batch.withSource());
                    exchange.setProperty("plcLogWithoutSource", batch.withoutSource());
                    exchange.setProperty("plcLogSkip", false);
                } catch (Exception e) {
                    exchange.setProperty(Exchange.EXCEPTION_CAUGHT, e);
                    exchange.setProperty("plcLogSkip", true);
                    logger.warn("Discarding MQTT-PLC-LOG inbound payload after transform failure: " + e.getMessage());
                    inboundErrorNotifier.recordError(exchange);
                }
            })
            .choice()
                .when(simple("${exchangeProperty.plcLogSkip} != true && ${exchangeProperty.plcLogWithSource} != null && ${exchangeProperty.plcLogWithSource.size} > 0"))
                    .to("direct:plc-log-ingest-with-source")
            .end()
            .choice()
                .when(simple("${exchangeProperty.plcLogSkip} != true && ${exchangeProperty.plcLogWithoutSource} != null && ${exchangeProperty.plcLogWithoutSource.size} > 0"))
                    .to("direct:plc-log-ingest-without-source")
            .end();

        from("direct:plc-log-ingest-with-source")
            .routeId("plc-log-ingest-with-source")
            .process(exchange -> exchange.getMessage().setBody(exchange.getProperty("plcLogWithSource")))
            .setProperty("protocolLabel").constant("MQTT-PLC-LOG")
            .setProperty("storeLogUri").simple("{{mqtt.read.store.log.uri}}")
            .setProperty("storeParameterUri").simple("{{mqtt.read.store.parameter.uri}}")
            .setProperty("afterStoreEnabled").simple("{{mqtt.read.afterStore.enabled}}")
            .setProperty("afterStoreUri").simple("{{mqtt.read.afterStore.uri}}")
            .to("direct:store-device-request-inbound");

        from("direct:plc-log-ingest-without-source")
            .routeId("plc-log-ingest-without-source")
            .doTry()
                .process(exchange -> exchange.getMessage().setBody(exchange.getProperty("plcLogWithoutSource")))
                .toD("{{plc.log.store.device.log.uri}}")
            .doCatch(Exception.class)
                .log(LoggingLevel.WARN,
                    "Discarding MQTT-PLC-LOG (no source) after DEVICE_LOG write failure: ${exception.message}")
                .process(inboundErrorNotifier::recordError)
            .end();
    }

    private void prepareInboundRows(Exchange exchange) {
        List<Map<String, Object>> rows = gatewayRequestService.coerceInboundRows(exchange.getMessage().getBody());
        exchange.getMessage().setBody(rows);
        exchange.setProperty("inboundPayload", rows);
        String purposeEnumId = rows.isEmpty() ? null : Objects.toString(rows.get(0).get("purposeEnumId"), null);
        exchange.setProperty("purposeEnumId", purposeEnumId);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> currentRows(Exchange exchange) {
        return (List<Map<String, Object>>) exchange.getMessage().getBody(List.class);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> propertyRows(Exchange exchange, String propertyName) {
        return (List<Map<String, Object>>) exchange.getProperty(propertyName);
    }
}
