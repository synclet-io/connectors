CREATE LOGIN synclet WITH PASSWORD = 'Synclet-Test1!', CHECK_POLICY = OFF;
CREATE LOGIN noperm WITH PASSWORD = 'NoPerm-Test1!', CHECK_POLICY = OFF;
CREATE LOGIN nouser WITH PASSWORD = 'NoUser-Test1!', CHECK_POLICY = OFF;
CREATE LOGIN colonly WITH PASSWORD = 'ColOnly-Test1!', CHECK_POLICY = OFF;
CREATE DATABASE synclet;
GO
USE synclet;
GO
CREATE USER synclet FOR LOGIN synclet;
ALTER ROLE db_datareader ADD MEMBER synclet;
ALTER ROLE db_ddladmin ADD MEMBER synclet;
ALTER ROLE db_datawriter ADD MEMBER synclet;
CREATE USER noperm FOR LOGIN noperm;
CREATE USER colonly FOR LOGIN colonly;
GO
CREATE SCHEMA other;
GO
CREATE SCHEMA twin_a;
GO
CREATE SCHEMA twin_b;
GO
CREATE SCHEMA colgrants;
GO
CREATE SCHEMA empty;
GO
CREATE SCHEMA oddnames;
GO
CREATE TYPE dbo.positive_int FROM int NOT NULL;
GO
CREATE TABLE dbo.users (
  id int PRIMARY KEY,
  name nvarchar(100),
  updated_at datetimeoffset(3)
);
INSERT INTO dbo.users VALUES
  (1, N'ann', '2024-01-01T00:00:00+00:00'),
  (2, N'bob', '2024-01-02T00:00:00+02:00'),
  (3, N'cé', '2024-01-03T00:00:00+00:00'),
  (4, NULL, NULL);
CREATE TABLE dbo.order_lines (
  order_id int NOT NULL,
  line int NOT NULL,
  qty int,
  CONSTRAINT pk_order_lines PRIMARY KEY (order_id, line)
);
INSERT INTO dbo.order_lines VALUES (1, 1, 5), (1, 2, 7);
CREATE TABLE dbo.events (
  id int PRIMARY KEY,
  updated_at datetime2(7),
  rv rowversion
);
CREATE TABLE other.hidden (id int);
INSERT INTO other.hidden VALUES (1);
CREATE TABLE twin_a.twin (id int);
INSERT INTO twin_a.twin VALUES (1);
CREATE TABLE twin_b.twin (id int);
INSERT INTO twin_b.twin VALUES (2);
CREATE TABLE colgrants.partial (id int PRIMARY KEY, visible nvarchar(20), secret nvarchar(20));
INSERT INTO colgrants.partial VALUES (1, N'shown', N'hidden');
CREATE TABLE colgrants.pk_hidden (id int PRIMARY KEY, name nvarchar(20));
INSERT INTO colgrants.pk_hidden VALUES (1, N'shown');
CREATE TABLE colgrants.none (id int);
CREATE TABLE oddnames.weird ([a.b] int, [a]]b] int, [[x]]] int);
INSERT INTO oddnames.weird VALUES (1, 2, 3);
GRANT SELECT ON oddnames.weird ([a.b]) TO colonly;
GRANT SELECT ON colgrants.partial (id, visible) TO colonly;
GRANT SELECT ON colgrants.pk_hidden (name) TO colonly;
CREATE TABLE dbo.types (
  id int PRIMARY KEY,
  c_bit bit,
  c_tinyint tinyint,
  c_smallint smallint,
  c_int int,
  c_bigint bigint,
  c_decimal decimal(38, 10),
  c_numeric numeric(10, 2),
  c_money money,
  c_smallmoney smallmoney,
  c_real real,
  c_float float,
  c_char char(4),
  c_varchar varchar(20),
  c_varchar_max varchar(max),
  c_nchar nchar(3),
  c_nvarchar nvarchar(20),
  c_text text,
  c_ntext ntext,
  c_xml xml,
  c_variant sql_variant,
  c_uuid uniqueidentifier,
  c_binary binary(3),
  c_varbinary varbinary(10),
  c_image image,
  c_rowversion rowversion,
  c_date date,
  c_time time(7),
  c_datetime datetime,
  c_datetime2 datetime2(7),
  c_smalldatetime smalldatetime,
  c_datetimeoffset datetimeoffset(7),
  c_hierarchyid hierarchyid,
  c_geography geography,
  c_geometry geometry,
  c_alias dbo.positive_int NULL,
  c_sysname sysname NULL
);
INSERT INTO dbo.types (id, c_bit, c_tinyint, c_smallint, c_int, c_bigint, c_decimal, c_numeric, c_money, c_smallmoney,
  c_real, c_float, c_char, c_varchar, c_varchar_max, c_nchar, c_nvarchar, c_text, c_ntext, c_xml, c_variant, c_uuid,
  c_binary, c_varbinary, c_image, c_date, c_time, c_datetime, c_datetime2, c_smalldatetime, c_datetimeoffset,
  c_hierarchyid, c_geography, c_geometry, c_alias, c_sysname)
VALUES (1, 1, 255, -32768, -2147483648, 9007199254740993, 1234567890123456789012345678.0123456789, -0.50,
  922337203685477.5807, -214748.3648, 0.1, 1.5, 'ab', 'varchar', REPLICATE(CONVERT(varchar(max), 'x'), 9000), N'nç', N'héllo',
  'text', N'ntëxt', '<a b="1">x</a>', CAST(42 AS int), '6F9619FF-8B86-D011-B42D-00C04FC964FF',
  0x00FF10, 0x00FF, 0x0102, '2024-02-29', '13:45:30.1234567', '2024-01-02T03:04:05.123', '2024-01-02T03:04:05.1234567',
  '2024-01-02T03:04:00', '2024-01-02T03:04:05.1234567+02:00',
  hierarchyid::Parse('/1/2/'), geography::STGeomFromText('POINT(30 10)', 4326), geometry::STGeomFromText('LINESTRING(0 0, 1 1)', 0),
  7, N'sys');
INSERT INTO dbo.types (id) VALUES (2);
GO
CREATE VIEW dbo.user_names AS SELECT id, name FROM dbo.users;
GO
