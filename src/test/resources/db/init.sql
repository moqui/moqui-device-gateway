-- =============================================================================
-- Moqui Device Gateway — minimal PostgreSQL schema
-- Tables required by the inbound MQTT route (PARAMETER_LOG / PARAMETER UPDATE)
-- and the outbound write / config-export routes.
-- =============================================================================

-- Moqui type → PostgreSQL mapping used here:
--   id            → VARCHAR(40)
--   id-long       → VARCHAR(255)
--   text-short    → VARCHAR(255)
--   text-medium   → VARCHAR(4000)
--   text-long     → TEXT
--   text-indicator→ CHAR(1)
--   number-integer→ INTEGER
--   number-decimal→ NUMERIC(22,6)
--   date-time     → TIMESTAMP

-- -----------------------------------------------------------------------------
-- ENUMERATION  (referenced by DEVICE_REQUEST, DEVICE_CONNECTION lookups)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ENUMERATION (
    ENUM_ID          VARCHAR(40)  PRIMARY KEY,
    ENUM_TYPE_ID     VARCHAR(40),
    PARENT_ENUM_ID   VARCHAR(40),
    ENUM_CODE        VARCHAR(255),
    SEQUENCE_NUM     INTEGER,
    DESCRIPTION      VARCHAR(4000),
    OPTION_VALUE     VARCHAR(255),
    OPTION_INDICATOR CHAR(1),
    RELATED_ENUM_ID  VARCHAR(40),
    RELATED_ENUM_TYPE_ID VARCHAR(40),
    STATUS_FLOW_ID   VARCHAR(40)
);

-- -----------------------------------------------------------------------------
-- DEVICE  (digital twin — referenced by PARAMETER.DEVICE_ID)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEVICE (
    DEVICE_ID            VARCHAR(40)  PRIMARY KEY,
    PARENT_DEVICE_ID     VARCHAR(40),
    DEVICE_TYPE_ENUM_ID  VARCHAR(40)  NOT NULL,
    PURPOSE_ENUM_ID      VARCHAR(40),
    STATUS_ID            VARCHAR(40),
    STATUS_FLOW_ID       VARCHAR(40),
    DESCRIPTION          TEXT,
    SERIAL_NUMBER        VARCHAR(255),
    UUID                 VARCHAR(255),
    FROM_DATE            TIMESTAMP,
    THRU_DATE            TIMESTAMP,
    LAST_UPDATED_STAMP   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- PHYSICAL_DEVICE  (used in device-config export query for DEVICE_NAME)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS PHYSICAL_DEVICE (
    DEVICE_ID            VARCHAR(40)  PRIMARY KEY,
    DEVICE_NAME          VARCHAR(255),
    VERSION              VARCHAR(255),
    HARDWARE_VERSION     VARCHAR(255),
    FIRMWARE_VERSION     VARCHAR(255),
    OPERATING_SYSTEM     VARCHAR(255),
    SOFTWARE_APPLICATION VARCHAR(255),
    CYCLE_TIME           INTEGER,
    IS_MULTICORE         CHAR(1)
);

-- -----------------------------------------------------------------------------
-- DEVICE_GROUP / DEVICE_GROUP_MEMBER  (gateway membership and startup restore)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEVICE_GROUP (
    DEVICE_ID            VARCHAR(40) PRIMARY KEY,
    GROUP_NAME           VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS DEVICE_GROUP_MEMBER (
    DEVICE_ID            VARCHAR(40) NOT NULL,
    MEMBER_DEVICE_ID     VARCHAR(40) NOT NULL,
    PURPOSE_ENUM_ID      VARCHAR(40),
    SEQUENCE_NUM         INTEGER,
    DESCRIPTION          VARCHAR(4000),
    STATUS_ID            VARCHAR(40),
    STATUS_FLOW_ID       VARCHAR(40),
    PRIMARY KEY (DEVICE_ID, MEMBER_DEVICE_ID)
);

-- -----------------------------------------------------------------------------
-- DEVICE_LOG  (PLC diagnostic log batches — moqui.device.DeviceLog)
-- Stores CODESYS LoggerFacade entries: deviceId=loggerName, payload=JSON entry.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEVICE_LOG (
    DEVICE_LOG_ID        VARCHAR(40)  PRIMARY KEY,
    DEVICE_ID            VARCHAR(40)  NOT NULL,
    PAYLOAD              TEXT         NOT NULL,
    SEQUENCE_NUM         INTEGER,
    OBSERVED_DATE        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS DEVLOG_TS ON DEVICE_LOG (DEVICE_ID, OBSERVED_DATE DESC);

-- -----------------------------------------------------------------------------
-- PARAMETER_DEF  (referenced by PARAMETER)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS PARAMETER_DEF (
    PARAMETER_DEF_ID     VARCHAR(40)  PRIMARY KEY,
    PARENT_PARAMETER_DEF_ID VARCHAR(40),
    PARAMETER_TYPE_ENUM_ID VARCHAR(40),
    PURPOSE_ENUM_ID      VARCHAR(40),
    PARAMETER_CODE       VARCHAR(255),
    PARAMETER_NAME       VARCHAR(255),
    DESCRIPTION          TEXT,
    MIN_VALUE            NUMERIC(22,6),
    MAX_VALUE            NUMERIC(22,6),
    DEFAULT_VALUE        VARCHAR(255),
    IS_REQUIRED          CHAR(1)
);

-- -----------------------------------------------------------------------------
-- PARAMETER  (current logical state — upserted by mqtt.read.store.parameter.uri)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS PARAMETER (
    PARAMETER_ID         VARCHAR(40)  PRIMARY KEY,
    PARAMETER_DEF_ID     VARCHAR(40),
    PARAMETER_ALIAS      VARCHAR(255),
    SEQUENCE_NUM         INTEGER,
    PARAMETER_UOM_ID     VARCHAR(40),
    NUMERIC_VALUE        NUMERIC(22,6),
    SYMBOLIC_VALUE       VARCHAR(255),
    PARAMETER_ENUM_ID    VARCHAR(40),
    DEVICE_ID            VARCHAR(40),
    DEVICE_CONFIG_ID     VARCHAR(40),
    LAST_UPDATED_STAMP   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS PARAM_DEVICE  ON PARAMETER (DEVICE_ID);
CREATE INDEX IF NOT EXISTS PARAM_CONFIG  ON PARAMETER (DEVICE_CONFIG_ID);

-- -----------------------------------------------------------------------------
-- PARAMETER_LOG  (telemetry time-series — inserted by mqtt.read.store.log.uri)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS PARAMETER_LOG (
    PARAMETER_LOG_ID     VARCHAR(40)  PRIMARY KEY,
    PARAMETER_ID         VARCHAR(40),
    SEQUENCE_NUM         BIGINT,
    OBSERVED_DATE        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    NUMERIC_VALUE        NUMERIC(22,6),
    SYMBOLIC_VALUE       VARCHAR(255),
    PARAMETER_ENUM_ID    VARCHAR(40)
);

CREATE INDEX IF NOT EXISTS PARAMLOG_PARAM ON PARAMETER_LOG (PARAMETER_ID);
CREATE INDEX IF NOT EXISTS PARAMLOG_TS    ON PARAMETER_LOG (OBSERVED_DATE DESC);

-- -----------------------------------------------------------------------------
-- ENTITY_AUDIT_LOG  (used by onlyChangedParameters filter in write queries)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ENTITY_AUDIT_LOG (
    AUDIT_HISTORY_SEQ_ID VARCHAR(40)  PRIMARY KEY,
    CHANGED_ENTITY_NAME  VARCHAR(255),
    CHANGED_FIELD_NAME   VARCHAR(255),
    PK_PRIMARY_VALUE     VARCHAR(255),
    PK_SECONDARY_VALUE   VARCHAR(255),
    PK_REST_COMBINED_VALUE VARCHAR(255),
    OLD_VALUE_TEXT       TEXT,
    NEW_VALUE_TEXT       TEXT,
    CHANGE_REASON        VARCHAR(4000),
    CHANGED_DATE         TIMESTAMP,
    CHANGED_BY_USER_ID   VARCHAR(40),
    CHANGED_IN_VISIT_ID  VARCHAR(40)
);

CREATE INDEX IF NOT EXISTS ENTAUDLOG_ENTPKPR ON ENTITY_AUDIT_LOG (CHANGED_ENTITY_NAME, PK_PRIMARY_VALUE);
CREATE INDEX IF NOT EXISTS ENTAUDLOG_PKPRIM  ON ENTITY_AUDIT_LOG (PK_PRIMARY_VALUE, CHANGED_DATE);

-- -----------------------------------------------------------------------------
-- DEVICE_CONNECTION  (transport config — queried by gateway.request.sql)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEVICE_CONNECTION (
    CONNECTION_NAME      VARCHAR(255) PRIMARY KEY,
    DEVICE_ID            VARCHAR(40)  NOT NULL,
    CONNECTION_TYPE_ENUM_ID VARCHAR(40),
    PURPOSE_ENUM_ID      VARCHAR(40),
    DESCRIPTION          VARCHAR(4000),
    USER_ID              VARCHAR(40),
    DRIVER_ENUM_ID       VARCHAR(40)  NOT NULL,
    TRANSPORT_ENUM_ID    VARCHAR(40),
    TRANSPORT_CONFIG     VARCHAR(4000),
    OPTIONS              VARCHAR(255)
);

-- -----------------------------------------------------------------------------
-- DEVICE_REQUEST  (queried by gateway.request.sql and write SQL)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEVICE_REQUEST (
    REQUEST_NAME         VARCHAR(255) PRIMARY KEY,
    PARENT_REQUEST_NAME  VARCHAR(255),
    DEVICE_ID            VARCHAR(40)  NOT NULL,
    REQUEST_TYPE_ENUM_ID VARCHAR(40),
    PURPOSE_ENUM_ID      VARCHAR(40),
    REQUEST_GROUP        VARCHAR(255),
    PRIORITY             INTEGER,
    SEQUENCE_NUM         INTEGER,
    DESCRIPTION          VARCHAR(4000),
    RUN_SERVICE_NAME     VARCHAR(4000),
    ONLY_CHANGED_PARAMETERS CHAR(1)   DEFAULT 'Y',
    FROM_DATE            TIMESTAMP,
    THRU_DATE            TIMESTAMP,
    ROUTER_ENUM_ID       VARCHAR(40)  NOT NULL,
    CONNECTION_NAME      VARCHAR(255),
    BROKER_URI           VARCHAR(4000),
    TIMEOUT              INTEGER,
    POLLING_INTERVAL     INTEGER,
    QOS                  INTEGER,
    RETAINED             CHAR(1)      DEFAULT 'N',
    QUERY                VARCHAR(4000),
    LAST_UPDATED_STAMP   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- DEVICE_REQUEST_ITEM  (queried by gateway.request.items.sql and write SQL)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEVICE_REQUEST_ITEM (
    REQUEST_NAME         VARCHAR(255) NOT NULL,
    REQUEST_ITEM_NAME    VARCHAR(255),
    PARAMETER_ID         VARCHAR(40)  NOT NULL,
    SEQUENCE_NUM         INTEGER,
    QUERY                TEXT,
    ITEM_TYPE_ENUM_ID    VARCHAR(40),
    MIN_ITEM_VALUE       NUMERIC(22,6),
    MAX_ITEM_VALUE       NUMERIC(22,6),
    DEFAULT_ITEM_VALUE   VARCHAR(255),
    ITEM_VALUE           VARCHAR(255),
    ALLOW_DUPLICATE      CHAR(1)      DEFAULT 'N',
    TOLERANCE            NUMERIC(22,6) DEFAULT 0.0,
    SCALING_FACTOR       NUMERIC(22,6) DEFAULT 1.0,
    OFFSET_VALUE         NUMERIC(22,6) DEFAULT 0.0,
    SIGNIFICANT_DIGITS   INTEGER      DEFAULT 2,
    REVERSE_LOGIC        CHAR(1),
    FORMAT               VARCHAR(255),
    MASK                 VARCHAR(255),
    PRIMARY KEY (REQUEST_NAME, PARAMETER_ID)
);

-- -----------------------------------------------------------------------------
-- DEVICE_CONFIG  (used by device-config export route)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEVICE_CONFIG (
    DEVICE_CONFIG_ID     VARCHAR(40)  PRIMARY KEY,
    PARENT_CONFIG_ID     VARCHAR(40),
    CONFIG_TYPE_ENUM_ID  VARCHAR(40),
    PURPOSE_ENUM_ID      VARCHAR(40),
    DEVICE_TYPE_ENUM_ID  VARCHAR(40)  NOT NULL,
    CONFIG_NAME          VARCHAR(255) NOT NULL,
    DESCRIPTION          VARCHAR(4000),
    FROM_DATE            TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    THRU_DATE            TIMESTAMP,
    CONTROL_METHOD_ENUM_ID   VARCHAR(40),
    APPROXIMATED_FUNCTION_ID VARCHAR(40)
);

CREATE UNIQUE INDEX IF NOT EXISTS DEVCONF_VERS ON DEVICE_CONFIG
    (DEVICE_TYPE_ENUM_ID, CONFIG_NAME, FROM_DATE);

-- -----------------------------------------------------------------------------
-- DEVICE_RULE_SET  (used by device-config export route)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEVICE_RULE_SET (
    DEVICE_RULE_SET_ID   VARCHAR(40)  PRIMARY KEY,
    PARENT_RULE_SET_ID   VARCHAR(40),
    PURPOSE_ENUM_ID      VARCHAR(40),
    DEVICE_ID            VARCHAR(40)  NOT NULL,
    SEQUENCE_NUM         INTEGER,
    RULE_SET_NAME        VARCHAR(255) NOT NULL,
    DESCRIPTION          VARCHAR(4000),
    FROM_DATE            TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    THRU_DATE            TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS DEVRSET_VERS ON DEVICE_RULE_SET
    (DEVICE_ID, RULE_SET_NAME, FROM_DATE);

-- -----------------------------------------------------------------------------
-- DEVICE_RULE  (used by device-config export route)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEVICE_RULE (
    DEVICE_RULE_ID       VARCHAR(40)  PRIMARY KEY,
    PARENT_RULE_ID       VARCHAR(40),
    RULE_TYPE_ENUM_ID    VARCHAR(40),
    DEVICE_RULE_SET_ID   VARCHAR(40)  NOT NULL,
    DEVICE_CONFIG_ID     VARCHAR(40)  NOT NULL,
    DEVICE_ID            VARCHAR(40)  NOT NULL,
    STATUS_ID            VARCHAR(40),
    STATUS_FLOW_ID       VARCHAR(40),
    RULE_NAME            VARCHAR(255) NOT NULL,
    DESCRIPTION          VARCHAR(4000),
    PRIORITY             INTEGER      NOT NULL,
    SERVICE_NAME         VARCHAR(4000),
    IS_ENABLED           CHAR(1)      DEFAULT 'Y',
    FROM_DATE            TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    THRU_DATE            TIMESTAMP,
    RUN_DEVICE           CHAR(1)
);

CREATE INDEX IF NOT EXISTS DEVRULE_SCOPE ON DEVICE_RULE
    (DEVICE_ID, STATUS_FLOW_ID, STATUS_ID, FROM_DATE);

-- -----------------------------------------------------------------------------
-- moqui-math trajectory tables  (used by device-config export trajectory UNION)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS APPROXIMATED_FUNCTION_SAMPLE (
    APPROXIMATED_FUNCTION_ID        VARCHAR(40) NOT NULL,
    APPROXIMATED_FUNCTION_SAMPLE_ID VARCHAR(40) NOT NULL,
    SEQUENCE_NUM                    INTEGER,
    SAMPLE_VECTOR_ID                VARCHAR(40),
    PRIMARY KEY (APPROXIMATED_FUNCTION_ID, APPROXIMATED_FUNCTION_SAMPLE_ID)
);

CREATE TABLE IF NOT EXISTS TRAJECTORY_POINT (
    APPROXIMATED_FUNCTION_ID        VARCHAR(40) NOT NULL,
    APPROXIMATED_FUNCTION_SAMPLE_ID VARCHAR(40) NOT NULL,
    IS_BREAK_POINT                  CHAR(1)        DEFAULT 'N',
    BLENDING_ENUM_ID                VARCHAR(40),
    POINT_TIME_OFFSET_MILLIS        NUMERIC(22,6),
    PRIMARY KEY (APPROXIMATED_FUNCTION_ID, APPROXIMATED_FUNCTION_SAMPLE_ID)
);

CREATE TABLE IF NOT EXISTS PARAMETRIC_PATH_POINT (
    APPROXIMATED_FUNCTION_ID        VARCHAR(40) NOT NULL,
    APPROXIMATED_FUNCTION_SAMPLE_ID VARCHAR(40) NOT NULL,
    TOLERANCE                       NUMERIC(22,6),
    PRIMARY KEY (APPROXIMATED_FUNCTION_ID, APPROXIMATED_FUNCTION_SAMPLE_ID)
);

CREATE TABLE IF NOT EXISTS VECTOR (
    VECTOR_ID VARCHAR(40) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS VECTOR_COMPONENT (
    VECTOR_ID          VARCHAR(40)   NOT NULL,
    VECTOR_COMPONENT_ID VARCHAR(40)  NOT NULL,
    DIMENSION_INDEX    INTEGER,
    REAL_VALUE         NUMERIC(22,6),
    PRIMARY KEY (VECTOR_ID, VECTOR_COMPONENT_ID)
);
