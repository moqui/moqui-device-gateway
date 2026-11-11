/*
This software is in the public domain under CC0 1.0 Universal plus a
Grant of Patent License.

To the extent possible under law, the author(s) have dedicated all
copyright and related and neighboring rights to this software to the
public domain worldwide. This software is distributed without any
warranty.

You should have received a copy of the CC0 Public Domain Dedication
along with this software (see the LICENSE.md file). If not, see
<http://creativecommons.org/publicdomain/zero/1.0/>.
*/
const dir = System.getProperty("gateway_acceptance_control_dir")
const phase = System.getProperty("gateway_acceptance_phase") || "main"
const SUB = "GatewayAcceptanceSubscribe"
const PUB = "GatewayAcceptancePublish"
const TOPIC = "mqtt-read-device-request/acceptance/recovery"
let serial = 0
const file = name => ec.resource.getLocationReference(dir + "/" + name)
function waitFor(test, timeout = 90000) {
    const until = Date.now() + timeout
    while (Date.now() < until) {
        try {
            const result = test()
            if (result) return result
        } catch (_) {
            ec.message.clearErrors()
        }
        control("wait", {seconds: 0.05})
    }
    throw new Error("Timed out waiting for acceptance condition")
}
function control(action, extra = {}) {
    const id = ++serial
    // A resource write consults DB-backed locks; outage control must use a DB-independent channel.
    const result = JSON.parse(
        ec.resource
            .getLocationReference(
                System.getProperty("gateway_acceptance_control_url") +
                    encodeURIComponent(JSON.stringify({id, phase, action, ...extra}))
            )
            .getText()
    )
    if (!result.ok) throw new Error(result.error)
    return result
}
const count = () =>
    ec.entity.find("moqui.math.ParameterLog").condition("parameterId", "TestPlcFeedbackParam").useCache(false).count()
const seen = value =>
    ec.entity
        .find("moqui.math.ParameterLog")
        .condition("parameterId", "TestPlcFeedbackParam")
        .condition("numericValue", value)
        .useCache(false)
        .count() > 0
const inRange = (start, n) =>
    ec.entity
        .find("moqui.math.ParameterLog")
        .condition("parameterId", "TestPlcFeedbackParam")
        .condition("numericValue", "greater-equals", start)
        .condition("numericValue", "less", start + n)
        .useCache(false)
        .count()
const notices = () =>
    ec.entity
        .find("moqui.security.user.NotificationMessage")
        .condition("topic", "in", ["DeviceInboundFailed", "DeviceInboundRecovered"])
        .useCache(false)
        .count()
spec({
    setup() {
        if (!dir) throw new Error("Use scripts/run-acceptance.py: a private controller/runtime is required")
        ec.artifactExecution.disableAuthz()
        if (phase !== "main") return
        for (const row of ec.entity.find("moqui.device.DeviceRequest").list()) {
            // the seeded subscriptions are not started: only this spec's request runs
            if (["DrtSubscribe", "DrtCyclic", "DrtEvent", "DrtStateChange"].includes(row.requestTypeEnumId)) {
                row.thruDate = ec.user.nowTimestamp
                row.update()
            }
        }
        const common = {
            deviceId: "VIRTUAL_PLC_01",
            routerEnumId: "DrrMQTTBroker",
            runServiceName: "moqui.device.gateway.DeviceGatewayServices.run#DeviceGatewayRequest",
            brokerUri: System.getProperty("gateway_acceptance_broker_uri"),
            query: TOPIC,
            timeout: 5000
        }
        call("create#moqui.device.DeviceRequest", {
            ...common,
            requestName: SUB,
            requestTypeEnumId: "DrtSubscribe",
            purposeEnumId: "DrpLogging"
        })
        call("create#moqui.device.DeviceRequestItem", {
            requestName: SUB,
            parameterId: "TestPlcFeedbackParam",
            requestItemName: "value",
            query: TOPIC,
            itemTypeEnumId: "DritReal",
            allowDuplicate: "Y"
        })
        call("create#moqui.device.DeviceRequest", {...common, requestName: PUB, requestTypeEnumId: "DrtWrite"})
    },
    cleanup() {
        ec.artifactExecution.enableAuthz()
    }
})
feature("main: broker loss, network cut, database crash, database stall and sustained collection lose nothing", {
    ignore: phase !== "main",
    when() {
        call("moqui.device.DeviceServices.run#DeviceRequest", {requestName: SUB})
        control("publish", {topic: TOPIC, value: 11})
        waitFor(() => seen(11))
        control("broker_stop")
        control("wait", {seconds: 1})
        control("broker_start")
        control("publish", {topic: TOPIC, value: 12, repeat: 20})
        waitFor(() => seen(12))
        // The factory keeps publishing to its broker while the datacenter link is down; the session keeps them.
        control("net_cut")
        try {
            control("burst", {topic: TOPIC, start: 50000, count: 300})
            control("wait", {seconds: 2})
        } finally {
            control("net_restore")
        }
        waitFor(() => inRange(50000, 300) >= 300)
        const networkCut = inRange(50000, 300)
        const beforeNotices = notices()
        control("db_stop")
        try {
            control("publish", {topic: TOPIC, value: 13, repeat: 3})
            control("wait", {seconds: 2})
        } finally {
            control("db_start")
        }
        control("publish", {topic: TOPIC, value: 14, repeat: 5})
        waitFor(() => seen(13) && seen(14) && notices() > beforeNotices)
        const outage = control("evidence")
        control("db_pause")
        try {
            control("burst", {topic: TOPIC, start: 100000, count: 2200})
            control("wait", {seconds: 2})
        } finally {
            control("db_resume")
        }
        waitFor(() => inRange(100000, 2200) >= 2200, 180000)
        const stall = control("evidence")
        const start = Date.now(),
            initial = count()
        control("burst", {topic: TOPIC, start: 200000, count: 1000})
        waitFor(() => count() >= initial + 1000)
        const millis = Date.now() - start
        const result = {
            outageLogged: outage.storeWaits > 0,
            recoveryNotified: notices() > beforeNotices,
            crashSamplesKept: seen(13),
            networkCut,
            stalled: inRange(100000, 2200),
            queueDrops: stall.queueDrops,
            persisted: count() - initial,
            throughputMessages: 1000,
            elapsedMillis: millis,
            persistedPerSecond: 1000000 / Math.max(1, millis)
        }
        file("metrics.json").putText(JSON.stringify(result))
        return result
    },
    then(r) {
        r.outageLogged == true
        r.recoveryNotified == true
        r.crashSamplesKept == true
        r.networkCut == 300
        r.stalled == 2200
        r.queueDrops == 0
        r.persisted == 1000
    }
})
feature("restart: persisted requests restore subscriptions in a new native host process", {
    ignore: phase !== "restart",
    when() {
        // Published while no gateway ran: the persistent session kept them in the broker.
        control("burst", {topic: TOPIC, start: 400000, count: 200})
        const restored = call("moqui.device.DeviceAcquisitionServices.start#Subscriptions", {retryDelayMillis: 0})
        control("publish", {topic: TOPIC, value: 300001, repeat: 3})
        waitFor(() => seen(300001) && inRange(400000, 200) >= 200)
        return {restored, received: seen(300001), whileDown: inRange(400000, 200)}
    },
    then(r) {
        // started, not unchanged: nothing ran in the new process before the restoration
        r.restored.startedRequestNames.includes(SUB) == true
        r.received == true
        r.whileDown == 200
    }
})
