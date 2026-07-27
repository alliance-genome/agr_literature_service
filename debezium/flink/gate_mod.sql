-- SCRUM-6336 P2 gate: prove Debezium 3.5 -> Flink debezium-json -> ES-7 upsert + delete-by-id.
-- One tiny table (public.mod). Not part of the real pipeline; dropped after the gate passes.
SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE mod_src (
  mod_id INT,
  abbreviation STRING,
  short_name STRING,
  full_name STRING,
  PRIMARY KEY (mod_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'abc.public.mod',
  'properties.bootstrap.servers' = 'dbz_kafka:9092',
  'properties.group.id' = 'flink-gate-mod',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE mod_es (
  mod_id INT,
  abbreviation STRING,
  short_name STRING,
  full_name STRING,
  PRIMARY KEY (mod_id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'gate_mod_test'
);

INSERT INTO mod_es SELECT mod_id, abbreviation, short_name, full_name FROM mod_src;
