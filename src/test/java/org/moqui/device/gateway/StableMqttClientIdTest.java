package org.moqui.device.gateway;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.moqui.device.gateway.service.GatewayRequestService;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;

@QuarkusTest
@TestProfile(LocalNoInfraTestProfile.class)
class StableMqttClientIdTest {

    @Inject
    GatewayRequestService gatewayRequestService;

    @Test
    void buildMqttConsumerUriAddsStableClientIdAndReconnectDefaults() {
        String uri = gatewayRequestService.buildMqttConsumerUri(
            "paho-mqtt5:?brokerUrl=tcp://localhost:1883&userName=artemis&password=artemis",
            "plc/linea01/status",
            "GW_EDGE_01",
            "PLC_LINEA_01_MQTT_SUB",
            0
        );

        assertTrue(uri.contains("clientId=moqui-gw-GW_EDGE_01-PLC_LINEA_01_MQTT_SUB-0"));
        assertTrue(uri.contains("automaticReconnect=true"));
        assertTrue(uri.contains("cleanStart=false"));
        assertTrue(uri.contains("sessionExpiryInterval=86400"));
        assertTrue(uri.contains("maxReconnectDelay=5000"));
    }

    @Test
    void buildMqttConsumerUriKeepsExistingClientId() {
        String uri = gatewayRequestService.buildMqttConsumerUri(
            "paho-mqtt5:?brokerUrl=tcp://localhost:1883&clientId=custom-client",
            "plc/linea01/status",
            "GW_EDGE_01",
            "PLC_LINEA_01_MQTT_SUB",
            0
        );

        assertTrue(uri.contains("clientId=custom-client"));
    }

    @Test
    void subscribeMqttFailsInsteadOfReturningCompletedWhenNoRouteCanStart() {
        GatewayRequestService.RequestContext context = new GatewayRequestService.RequestContext(
            "REQ_BAD_ROUTE",
            "PLC_LINEA_01",
            null,
            "DrtCyclic",
            "DrpMonitoring",
            "DrrMoquiDeviceGateway",
            null,
            "bad-component:?",
            null,
            100,
            null,
            "N",
            null,
            null,
            null,
            null,
            null,
            null,
            List.of(new GatewayRequestService.RequestItem(
                "PARAM_1", "status", 1, null, null, null, "plc/linea01/status"))
        );

        assertThrows(IllegalStateException.class, () -> gatewayRequestService.subscribeMqtt(context));
    }
}
