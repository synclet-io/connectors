SET NAMES utf8mb4;
SET SESSION sql_mode = '';

CREATE TABLE types (
    id                INT PRIMARY KEY,
    c_bool_int        TINYINT(1),
    c_tinyint         TINYINT,
    c_smallint        SMALLINT,
    c_mediumint       MEDIUMINT,
    c_int             INT,
    c_bigint          BIGINT,
    c_bigint_unsigned BIGINT UNSIGNED,
    c_year            YEAR,
    c_decimal         DECIMAL(65, 30),
    c_numeric         NUMERIC(10, 2),
    c_float           FLOAT,
    c_double          DOUBLE,
    c_bit1            BIT(1),
    c_bit12           BIT(12),
    c_bit64           BIT(64),
    c_char            CHAR(5),
    c_varchar         VARCHAR(50),
    c_tinytext        TINYTEXT,
    c_text            TEXT,
    c_mediumtext      MEDIUMTEXT,
    c_longtext        LONGTEXT,
    c_enum            ENUM('small', 'it''s big'),
    c_set             SET('a', 'b', 'c'),
    c_json            JSON,
    c_binary          BINARY(3),
    c_varbinary       VARBINARY(10),
    c_tinyblob        TINYBLOB,
    c_blob            BLOB,
    c_mediumblob      MEDIUMBLOB,
    c_longblob        LONGBLOB,
    c_date            DATE,
    c_datetime        DATETIME(6),
    c_datetime0       DATETIME,
    c_timestamp       TIMESTAMP(3) NULL,
    c_time            TIME,
    c_time_neg        TIME(2),
    c_geometry        GEOMETRY,
    c_point           POINT,
    c_linestring      LINESTRING,
    c_polygon         POLYGON
) CHARACTER SET utf8mb4;

SET time_zone = '+02:00';
INSERT INTO types VALUES (
    1, 1, -128, 32767, 8388607, -2147483648, 9223372036854775807, 18446744073709551615, 2024,
    12345678901234567890123456789012345.123456789012345678901234567890, 0.10, 1.5, 3.141592653589793,
    b'1', b'101010101010', b'1111111111111111111111111111111111111111111111111111111111111111',
    'ab', 'héllo 🎉', 'tiny', 'text', 'medium', 'long',
    'it''s big', 'a,c', '{"b": [1, 2], "a": 1.5}',
    X'00FF10', X'DEADBEEF', X'00', 'blob', 'mediumblob', 'longblob',
    '2024-02-29', '2024-02-29 13:14:15.123456', '2024-02-29 13:14:15', '2024-02-29 13:14:15.120',
    '838:59:59', '-01:02:03.50',
    ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(3 4)'),
    ST_GeomFromText('LINESTRING(0 0,1 1)'), ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))')
);
SET time_zone = '+00:00';
INSERT INTO types (id) VALUES (2);

CREATE TABLE zero_dates (
    id INT PRIMARY KEY,
    d  DATE,
    dt DATETIME,
    ts TIMESTAMP NULL
);
INSERT INTO zero_dates VALUES
    (1, '2024-01-02', '2024-01-02 03:04:05', '2024-01-02 03:04:05'),
    (2, '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00'),
    (3, '2024-00-15', '2024-03-00 10:00:00', NULL);

CREATE TABLE `we.ird``name` (
    id      INT PRIMARY KEY,
    `a.b`   VARCHAR(10),
    `up``d` INT
);
INSERT INTO `we.ird``name` VALUES (1, 'x', 10), (2, 'y', 20), (3, 'z', 30);

CREATE TABLE users (
    id         INT PRIMARY KEY,
    name       VARCHAR(50),
    updated_at DATETIME(6)
);
INSERT INTO users VALUES (1, 'alice', '2024-01-01 00:00:00'), (2, 'bob', '2024-01-02 00:00:00');

CREATE TABLE secret (id INT PRIMARY KEY);

CREATE USER 'nodb'@'%' IDENTIFIED BY 'nodb';
CREATE USER 'noselect'@'%' IDENTIFIED BY 'noselect';
GRANT SELECT ON testdb.users TO 'noselect'@'%';
GRANT INSERT ON testdb.secret TO 'noselect'@'%';
CREATE USER 'norepl'@'%' IDENTIFIED BY 'norepl';
GRANT SELECT ON testdb.* TO 'norepl'@'%';

CREATE TABLE padded (
    id INT PRIMARY KEY,
    a  INT(5) ZEROFILL,
    b  DECIMAL(6, 2) ZEROFILL,
    f  FLOAT ZEROFILL
);
INSERT INTO padded VALUES (1, 42, 1.5, 2.5), (2, 0, 0.5, 0);

CREATE TABLE bin_events (
    id INT PRIMARY KEY,
    vb VARBINARY(4),
    bt BIT(12)
);
INSERT INTO bin_events VALUES (1, X'0001', b'000000000001'), (2, X'FF00', b'111100000000'), (3, X'FF01', b'111100000001');
