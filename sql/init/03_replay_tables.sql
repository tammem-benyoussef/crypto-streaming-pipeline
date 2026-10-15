-- The replay schema gets the same tables, so the same Flink job can write to either.
CREATE TABLE IF NOT EXISTS replay.bars_1m       (LIKE live.bars_1m       INCLUDING ALL);
CREATE TABLE IF NOT EXISTS replay.latest_price  (LIKE live.latest_price  INCLUDING ALL);
CREATE TABLE IF NOT EXISTS replay.late_events   (LIKE live.late_events   INCLUDING ALL);
