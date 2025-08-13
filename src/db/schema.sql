PRAGMA journal_mode = WAL;

BEGIN;

CREATE TABLE IF NOT EXISTS sections
(
    term         INTEGER NOT NULL REFERENCES terms (id),
    crn          INTEGER NOT NULL,
    timestamp    INTEGER NOT NULL,
    latest       INTEGER NOT NULL CHECK (latest = 1 OR latest = 0),

    subject      TEXT    NOT NULL COLLATE NOCASE CHECK (length(subject) = 4),
    number       ANY     NOT NULL COLLATE NOCASE CHECK (typeof(number) = 'text' OR typeof(number) = 'integer'),
    span         TEXT    NOT NULL COLLATE NOCASE CHECK (length(span) = 0 OR (length(span) = 2 AND typeof(number) = 'integer')),
    section      TEXT    NOT NULL COLLATE NOCASE CHECK (length(section) != 0),

    "select"     INTEGER NOT NULL,
    sec_type     TEXT    NOT NULL,
    millicredits INTEGER NOT NULL,
    title        TEXT    NOT NULL,

    -- TODO add table of section times
    -- for now just using primary times
    days         INTEGER,
    time         TEXT,
    instructor   TEXT,
    date         TEXT    NOT NULL,
    location     TEXT,
    status       INTEGER NOT NULL,

    capacity     INTEGER NOT NULL,
    actual       INTEGER NOT NULL,
    remaining    INTEGER NOT NULL,
    wl_capacity  INTEGER NOT NULL,
    wl_actual    INTEGER NOT NULL,
    wl_remaining INTEGER NOT NULL,

    notes        TEXT,

    PRIMARY KEY (term, crn, timestamp)
) WITHOUT ROWID, STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS latest_sections_crn ON sections (term, crn) WHERE latest = 1;

CREATE TABLE IF NOT EXISTS subscriptions
(
    channel     INTEGER NOT NULL,
    term        INTEGER NOT NULL,
    crn         INTEGER NOT NULL,
    all_updates INTEGER NOT NULL CHECK (all_updates = 0 OR all_updates = 1),

    PRIMARY KEY (channel, term, crn)
    -- No foreign key to sections since we can't uniquely index sections on term,crn :(
) WITHOUT ROWID, STRICT;

CREATE TRIGGER IF NOT EXISTS sub_sec_exists
    BEFORE INSERT
    ON subscriptions
    WHEN (SELECT count(*)
          FROM sections sec INDEXED BY latest_sections_crn
          WHERE latest = 1
            AND sec.term = NEW.term
            AND sec.crn = NEW.crn) = 0
BEGIN
    SELECT RAISE(ABORT, 'no matching section found for term and crn');
END;

CREATE TABLE IF NOT EXISTS terms
(
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL COLLATE NOCASE
) WITHOUT ROWID, STRICT;

COMMIT;