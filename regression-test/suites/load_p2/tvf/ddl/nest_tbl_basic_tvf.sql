
CREATE TABLE nest_tbl_basic_tvf(
    id INT NOT NULL,
    city VARCHAR(100) NULL,
    code INT NULL,
    kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
    kd02 TINYINT         NOT NULL DEFAULT "1",
    kd03 SMALLINT        NOT NULL DEFAULT "2",
    kd04 INT             NOT NULL DEFAULT "3",
    kd05 BIGINT          NOT NULL DEFAULT "4",
    kd06 LARGEINT        NOT NULL DEFAULT "5",
    kd07 FLOAT           NOT NULL DEFAULT "6.0",
    kd08 DOUBLE          NOT NULL DEFAULT "7.0",
    kd09 DECIMAL         NOT NULL DEFAULT "888888888",
    kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
    kd11 DATE            NOT NULL DEFAULT "2023-08-24",
    kd12 DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
    kd14 DATETIMEV2      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kd15 CHAR(255)       NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
    kd16 VARCHAR(300)    NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
    kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
    kd18 JSON            NULL,
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 32
PROPERTIES(
    "replication_num" = "1"
);