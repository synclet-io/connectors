CREATE TYPE mood AS ENUM ('sad', 'happy');
CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);

CREATE TABLE public.types (
    id              integer PRIMARY KEY,
    c_numeric       numeric(30, 10),
    c_numeric_nan   numeric,
    c_float8        float8,
    c_float8_inf    float8,
    c_real          real,
    c_bigint        bigint,
    c_integer       integer,
    c_smallint      smallint,
    c_text          text,
    c_varchar       varchar(20),
    c_char          char(3),
    c_bytea         bytea,
    c_json          json,
    c_jsonb         jsonb,
    c_int_array     integer[],
    c_text_array    text[],
    c_numeric_array numeric[],
    c_bytea_array   bytea[],
    c_uuid          uuid,
    c_enum          mood,
    c_domain        positive_int,
    c_date          date,
    c_time          time,
    c_timetz        timetz,
    c_timestamp     timestamp,
    c_timestamptz   timestamptz,
    c_interval      interval,
    c_bool          boolean,
    c_point         point,
    c_inet          inet
);

INSERT INTO public.types VALUES (
    1,
    12345678901234567890.0123456789,
    'NaN',
    1.5,
    'Infinity',
    0.25,
    9007199254740993,
    -2147483648,
    32767,
    'héllo',
    'varchar',
    'ab',
    '\x00ff10',
    '{"a":1.10}',
    '{"b": [1, 2], "a": null}',
    '{1,2,3}',
    '{a,NULL}',
    '{1.5,NaN,-Infinity}',
    '{"\\x00ff"}',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    'happy',
    7,
    '2024-02-29',
    '13:45:30.5',
    '13:45:30+02',
    '2024-01-02 03:04:05.123456',
    '2024-01-02 03:04:05+02',
    '1 year 2 months 3 days 4 hours 5 minutes 6 seconds',
    true,
    '(1,2)',
    '192.168.0.1/24'
);
INSERT INTO public.types (id) VALUES (2);

CREATE TABLE public.users (
    id         integer PRIMARY KEY,
    name       text NOT NULL,
    updated_at timestamptz
);
INSERT INTO public.users VALUES
    (1, 'ada', '2024-01-01 00:00:00+00'),
    (2, 'grace', '2024-01-02 00:00:00+00'),
    (3, 'linus', '2024-01-02 00:00:00+00'),
    (4, 'ken', '2024-01-03 00:00:00+00');

CREATE TABLE public.order_lines (
    order_id integer,
    line     integer,
    sku      text,
    PRIMARY KEY (order_id, line)
);
INSERT INTO public.order_lines VALUES (1, 1, 'a'), (1, 2, 'b');

CREATE VIEW public.user_names AS SELECT id, name FROM public.users;

CREATE TABLE public.events (
    id         integer PRIMARY KEY,
    updated_at timestamptz
);

CREATE SCHEMA other;
CREATE TABLE other.hidden (id integer PRIMARY KEY);

CREATE SCHEMA private;
CREATE TABLE private.secret (id integer);

CREATE ROLE nousage LOGIN PASSWORD 'nousage';
CREATE ROLE noconnect LOGIN PASSWORD 'noconnect';
CREATE DATABASE restricted;
REVOKE CONNECT ON DATABASE restricted FROM PUBLIC;
