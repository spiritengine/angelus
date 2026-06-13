-- Two covering indexes that kill unindexed scans on the daemon's single event
-- loop. Both were proven on a py-spy flamegraph of the live daemon and verified
-- with EXPLAIN QUERY PLAN on a copy of the live DB; this migration is purely
-- additive (no query/schema changes, no existing index touched).
--
-- 1. observations_pending_triage_count (catalog.observations_pending_triage_count)
--    runs a correlated `NOT EXISTS` whose inner subquery filters
--    observation_triage by (observation_id = o.id AND status = 'success').
--    The only triage index that helps is idx_observation_triage_status (status
--    alone), so for each ready observation the planner scanned the whole triage
--    set and filtered by observation_id afterward -- O(n^2) over ~9,890 rows,
--    measured at 6,809 ms per call and the cause of the multi-second `angelus
--    health` stall. The (observation_id, status) order lets the anti-join seek
--    the exact (observation_id=?, status=?) pair: 6,809 ms -> 6.7 ms. Column
--    order matters -- observation_id is the correlation equality the subquery
--    leads with, status is the secondary equality, so observation_id must come
--    first.
CREATE INDEX idx_observation_triage_observation_id_status
    ON observation_triage (observation_id, status);

-- 2. ready_observations_for (catalog.ready_observations_for) filters
--    `o.status='ready' AND o.source=?` then `ORDER BY o.id LIMIT 20`. With only
--    idx_observations_status (status alone) the planner scanned the entire
--    `ready` set and filtered by source per row; the triage loop runs this tens
--    of times/sec. (source, status, id) lets the equality columns (source,
--    status) seek and the trailing id satisfy the `ORDER BY o.id LIMIT 20`
--    without a sort: 2.50 ms -> 0.05 ms. Order matters -- source then status are
--    the two equalities, and id last is what turns the ORDER BY into an
--    index walk rather than a temp-b-tree sort.
CREATE INDEX idx_observations_source_status_id
    ON observations (source, status, id);
