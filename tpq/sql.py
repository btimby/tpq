"""
SQL constants.

The below are templates used to generate SQL.
"""


EXIST = """
SELECT EXISTS (
    SELECT *
    FROM information_schema.tables
    WHERE table_name='poque_%(name)s'
)
"""

CREATE = """
DO $$ BEGIN

CREATE TABLE "poque_%(name)s" (
    id          bigserial       PRIMARY KEY,
    data        json            NOT NULL
);

END $$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS poque_notify_%(name)s() CASCADE;

CREATE FUNCTION poque_notify_%(name)s() RETURNS TRIGGER AS $$ BEGIN
    PERFORM pg_notify('%(name)s', '');
    RETURN null;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER poque_insert_%(name)s
AFTER INSERT ON "poque_%(name)s"
FOR EACH ROW
EXECUTE PROCEDURE poque_notify_%(name)s();
"""

PUT = """
INSERT INTO poque_%(name)s (data) VALUES (%(data)s) RETURNING id;
"""

GET = """
DELETE FROM poque_%(name)s
WHERE id = (
    SELECT id
    FROM poque_%(name)s
    ORDER BY id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING data;
"""

LEN = """
WITH queued AS (
    SELECT *
    FROM poque_%(name)s
    FOR UPDATE SKIP LOCKED
)
SELECT COUNT(*) FROM queued;
"""

DEL = """
WITH queued AS (
    SELECT id
    FROM poque_%(name)s
    FOR UPDATE SKIP LOCKED
)
DELETE FROM poque_%(name)s WHERE id IN (SELECT id FROM queued);
"""
