#!/usr/bin/env bash
set -euo pipefail

# Requires:
# - PostgreSQL initialized with the local test schema/seed or an equivalent Moqui DB.
# - ActiveMQ Artemis running with MQTT enabled.
#
# Standard Moqui Docker setup:
#   docker compose -f ../moqui-framework/docker/activemq-compose.yml -p moqui-gateway up -d
#
# The test JVM expects the MQTT broker on:
#   tcp://localhost:1883
# with credentials:
#   artemis / artemis

./gradlew integrationTest --tests '*MqttInboundIntegrationTest' -Dquarkus.profile=integration
./gradlew integrationTest --tests '*GatewaySeededRouteIntegrationTest' -Dquarkus.profile=integration
./gradlew integrationTest --tests '*OpcUaGatewayIntegrationTest' -Dquarkus.profile=integration
./gradlew integrationTest --tests '*PlcLogIngestIntegrationTest' -Dquarkus.profile=integration
