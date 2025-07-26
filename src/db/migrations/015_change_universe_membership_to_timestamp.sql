-- Migration 015: Change universe_membership and universe_membership_changes start_at, end_at, and effective_date columns to TIMESTAMP

-- 1. Change universe_membership columns
ALTER TABLE universe_membership ALTER COLUMN start_at TYPE TIMESTAMP USING start_at::timestamp;
ALTER TABLE universe_membership ALTER COLUMN end_at TYPE TIMESTAMP USING end_at::timestamp;

-- 2. Change PK if needed (should be fine as PK type change is implicit)

-- 3. Change universe_membership_changes columns
ALTER TABLE universe_membership_changes ALTER COLUMN effective_date TYPE TIMESTAMP USING effective_date::timestamp;

-- 4. Add a comment for reversibility
-- To reverse: change TIMESTAMP columns back to DATE using ::date
