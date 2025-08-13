use std::collections::HashMap;
use std::path::Path;

use rusqlite::fallible_iterator::FallibleIterator;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, Type, Value, ValueRef};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, ToSql, Transaction, named_params};

use crate::minerva::parser::{
    CourseNumber, Days, Milliunits, SectionData, Select, Status, Subject,
};
use chrono::{DateTime, Utc};
use rusqlite::config::DbConfig;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SectionRecord {
    pub term: u32,
    pub timestamp: DateTime<Utc>,
    pub inner: SectionData,
}

impl TryFrom<&Row<'_>> for SectionRecord {
    type Error = rusqlite::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let timestamp = DateTime::<Utc>::from_timestamp(row.get(2)?, 0).unwrap();

        let notes = row
            .get_ref(24)?
            .as_str_or_null()?
            .iter()
            .flat_map(|s| s.split('\x1E'))
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        Ok(SectionRecord {
            term: row.get(0)?,
            timestamp,
            inner: SectionData {
                crn: row.get(1)?,

                subject: row.get(4)?,
                number: CourseNumber::from_row_indices(row, 5, 6)?,
                section: row.get(7)?,

                select: row.get(8)?,
                sec_type: row.get(9)?,
                millicredits: row.get(10)?,
                title: row.get(11)?,

                days: row.get(12)?,
                time: row.get(13)?,
                instructor: row.get(14)?,
                date: row.get(15)?,
                location: row.get(16)?,
                status: row.get(17)?,

                capacity: row.get(18)?,
                actual: row.get(19)?,
                remaining: row.get(20)?,
                wl_capacity: row.get(21)?,
                wl_actual: row.get(22)?,
                wl_remaining: row.get(23)?,
                notes,
            },
        })
    }
}

impl SectionRecord {
    /// Checks for equivalency, ignoring `timestamp` and `select`, and assuming `term` and `crn`
    /// are unchanged (otherwise the sections may be different)
    pub fn is_equivalent(&self, other: Option<&Self>) -> bool {
        if let Some(prev) = other {
            let pd = &prev.inner;
            let nd = &self.inner;
            pd.subject == nd.subject
                && pd.number == nd.number
                && pd.section == nd.section
                && pd.sec_type == nd.sec_type
                && pd.millicredits == nd.millicredits
                && pd.title == nd.title
                && pd.days == nd.days
                && pd.time == nd.time
                && pd.capacity == nd.capacity
                && pd.actual == nd.actual
                && pd.remaining == nd.remaining
                && pd.wl_capacity == nd.wl_capacity
                && pd.wl_actual == nd.wl_actual
                && pd.wl_remaining == nd.wl_remaining
                && pd.instructor == nd.instructor
                && pd.date == nd.date
                && pd.location == nd.location
                && pd.status == nd.status
                && pd.notes == nd.notes
        } else {
            false
        }
    }
}

impl FromSql for Subject {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        value
            .as_str()?
            .try_into()
            .map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

impl ToSql for Subject {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(&self.0)))
    }
}

// impl FromSql for CourseNumber {
//     fn column_result(value: ValueRef) -> FromSqlResult<Self> {
//         match value {
//             ValueRef::Text(s) => Ok(Self::from(
//                 str::from_utf8(s)
//                     .map(|s| s.to_owned())
//                     .map_err(|e| FromSqlError::Other(Box::new(e)))?,
//             )),
//             ValueRef::Integer(i) => Ok(Self::Plain(i as u16)),
//             _ => Err(FromSqlError::InvalidType),
//         }
//     }
// }
//
// impl ToSql for CourseNumber {
//     fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
//         match self {
//             Self::Plain(n) => Ok(ToSqlOutput::Owned(Value::Integer(*n as i64))),
//             o => Ok(ToSqlOutput::Owned(Value::Text(o.to_string()))),
//         }
//     }
// }

impl CourseNumber {
    fn from_row_indices(
        row: &Row,
        num_idx: usize,
        span_idx: usize,
    ) -> Result<Self, rusqlite::Error> {
        match (row.get_ref(num_idx)?, row.get_ref(span_idx)?.as_str()?) {
            (ValueRef::Integer(n), "") => Ok(Self::Plain(n as u16)),
            (ValueRef::Integer(n), s) => Ok(Self::Spanned(
                n as u16,
                s.as_bytes().try_into().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(span_idx, Type::Text, Box::new(e))
                })?,
            )),
            (ValueRef::Text(_), "") => Ok(Self::Other(row.get(5)?)),
            (ValueRef::Text(_), _) => Err(rusqlite::Error::FromSqlConversionFailure(
                6,
                Type::Text,
                anyhow::anyhow!("Non-empty span for text-numbered section").into_boxed_dyn_error(),
            )),
            (col, _) => Err(rusqlite::Error::InvalidColumnType(
                5,
                "number".to_string(),
                col.data_type(),
            )),
        }
    }
    fn number_tosql(&self) -> ToSqlOutput<'_> {
        ToSqlOutput::Borrowed(match self {
            Self::Plain(n) => ValueRef::Integer(*n as i64),
            Self::Spanned(n, _) => ValueRef::Integer(*n as i64),
            Self::Other(n) => ValueRef::Text(n.as_bytes()),
        })
    }

    /// If `nullable` is true, a non-spanned number will return sqlite Null
    /// from this function, instead of the empty string. This can help with
    /// constructing multi-result queries, but should NOT be used for INSERTs
    /// or UPDATEs
    fn span_tosql(&self, nullable: bool) -> ToSqlOutput<'_> {
        ToSqlOutput::Borrowed(match (self, nullable) {
            (Self::Spanned(_, s), _) => ValueRef::Text(s),
            (_, false) => ValueRef::Text(b""),
            (_, true) => ValueRef::Null,
        })
    }
}

impl FromSql for Select {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        let i = value.as_i64()? as u8;
        i.try_into().map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

impl ToSql for Select {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer((*self as u8) as i64)))
    }
}

impl FromSql for Milliunits {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        let val = value.as_i64()?;
        if (0..=(u16::MAX as i64)).contains(&val) {
            Ok(Milliunits(val as u16))
        } else {
            Err(FromSqlError::OutOfRange(val))
        }
    }
}

impl ToSql for Milliunits {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer(self.0 as i64)))
    }
}

impl FromSql for Days {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        let oi = value.as_i64_or_null()?;

        match oi {
            None => Ok(Days(None)),
            Some(i) if (0..(1 << 8)).contains(&i) => {
                let arr = [
                    (i >> 0) & 1 == 1,
                    (i >> 1) & 1 == 1,
                    (i >> 2) & 1 == 1,
                    (i >> 3) & 1 == 1,
                    (i >> 4) & 1 == 1,
                    (i >> 5) & 1 == 1,
                    (i >> 6) & 1 == 1,
                ];
                Ok(Days(Some(arr)))
            }
            Some(i) => Err(FromSqlError::OutOfRange(i)),
        }
    }
}

impl ToSql for Days {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(match self.0 {
            Some(a) => Value::Integer(a.iter().rev().fold(0i64, |a, e| (a << 1) | (*e as i64))),
            None => Value::Null,
        }))
    }
}

impl FromSql for Status {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        let i = value.as_i64()? as u8;
        i.try_into().map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

impl ToSql for Status {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Integer((*self as u8) as i64)))
    }
}

// General operations

pub fn open_or_init<P: AsRef<Path>>(path: P, init: bool) -> Result<Connection, rusqlite::Error> {
    let flags = if init {
        OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
    } else {
        OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_READ_WRITE
    };
    let conn = Connection::open_with_flags(path, flags)?;
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, true)?;
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, true)?;
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_QPSG, false)?;
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, false)?;
    if init {
        conn.execute_batch(include_str!("schema.sql"))?;
    }
    Ok(conn)
}

/// Last section fetch time present in the table
pub fn last_update(conn: &Connection) -> Result<Option<DateTime<Utc>>, rusqlite::Error> {
    conn.query_row(
        "SELECT max(timestamp) FROM sections INDEXED BY latest_sections_crn WHERE latest = 1;",
        (),
        |row| {
            row.get_ref(0)?
                .as_i64_or_null()?
                .map(|ts| {
                    DateTime::from_timestamp(ts, 0)
                        .ok_or(rusqlite::Error::IntegralValueOutOfRange(0, ts))
                })
                .transpose()
        },
    )
}

pub struct DatabaseStats {
    pub last_update: DateTime<Utc>,
    pub sections: usize,
    pub section_records: usize,
    pub terms: usize,
    pub subs: usize,
    pub channels: usize,
}

/// Number of sections, historical sections, terms, subscriptions, and channels in the database
pub fn db_stats(connection: &Connection) -> Result<DatabaseStats, rusqlite::Error> {
    let first = connection.query_one(
        "SELECT max(timestamp), sum(latest), count(*), count(DISTINCT term) FROM sections;",
        (),
        |row| {
            Ok((
                DateTime::from_timestamp(row.get(0)?, 0)
                    .ok_or(rusqlite::Error::IntegralValueOutOfRange(0, row.get(0)?))?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
            ))
        },
    )?;
    let second = connection.query_one(
        "SELECT count(*), count(DISTINCT channel) FROM subscriptions;",
        (),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(DatabaseStats {
        last_update: first.0,
        sections: first.1,
        section_records: first.2,
        terms: first.3,
        subs: second.0,
        channels: second.1,
    })
}

// Section Records

pub fn latest_section_from_term_crn(
    trx: &Transaction,
    term: u32,
    crn: u32,
) -> Result<Option<SectionRecord>, rusqlite::Error> {
    trx.prepare_cached(
        "SELECT * FROM sections INDEXED BY latest_sections_crn
        WHERE latest = 1 AND term = ?1 AND crn = ?2;",
    )?
    .query_one((term, crn), |r| SectionRecord::try_from(r))
    .optional()
}

pub fn demote_latest_by_crn(
    trx: &Transaction,
    term: u32,
    crn: u32,
) -> Result<bool, rusqlite::Error> {
    trx.prepare_cached(
        "UPDATE sections INDEXED BY latest_sections_crn SET latest = 0
            WHERE latest = 1 AND term = ?1 AND crn = ?2;",
    )?
    .execute((term, crn))
    .and_then(|n| {
        if n <= 1 {
            Ok(n == 1)
        } else {
            Err(rusqlite::Error::StatementChangedRows(n))
        }
    })
}

pub fn demote_latest_by_number(
    trx: &Transaction,
    term: u32,
    subject: Subject,
    number: CourseNumber,
    section: String,
) -> Result<bool, rusqlite::Error> {
    trx.prepare_cached(
        "UPDATE sections INDEXED BY latest_sections_crn SET latest = 0
            WHERE latest = 1 AND term = ?1 AND subject = ?2 AND number = ?3 and span = ?4 and section = ?5;",
    )?
        .execute((term, subject, number.number_tosql(), number.span_tosql(false), section))
        .and_then(|n| {
            if n <= 1 {
                Ok(n == 1)
            } else {
                Err(rusqlite::Error::StatementChangedRows(n))
            }
        })
}

pub fn bump_timestamp(
    trx: &Transaction,
    term: u32,
    crn: u32,
    timestamp: DateTime<Utc>,
) -> Result<bool, rusqlite::Error> {
    trx.prepare_cached(
        "UPDATE sections INDEXED BY latest_sections_crn SET timestamp = ?3
            WHERE latest = 1 AND term = ?1 AND crn = ?2;",
    )?
    .execute((term, crn, timestamp.timestamp()))
    .and_then(|n| {
        if n <= 1 {
            Ok(n == 1)
        } else {
            Err(rusqlite::Error::StatementChangedRows(n))
        }
    })
}

pub fn insert_section(
    trx: &Transaction,
    SectionRecord {
        term,
        timestamp,
        inner: sd,
    }: &SectionRecord,
    is_latest: bool,
) -> Result<(), rusqlite::Error> {
    let notes = sd.notes.join("\x1E");

    trx.prepare_cached(
        "INSERT INTO sections VALUES
        (:term, :crn, :timestamp, :latest, :subject, :number, :span, :section,
        :select, :sec_type, :millicredits, :title,
        :days, :time, :instructor, :date, :location, :status,
        :capacity, :actual, :remaining, :wl_capacity, :wl_actual, :wl_remaining,
        :notes);",
    )?
    .execute(named_params! {
        ":term": term,
        ":timestamp": timestamp.timestamp(),
        ":crn": sd.crn,
        ":latest": is_latest,
        ":subject": sd.subject,
        ":number": sd.number.number_tosql(),
        ":span": sd.number.span_tosql(false),
        ":section": sd.section,
        ":select": sd.select,
        ":sec_type": sd.sec_type,
        ":millicredits": sd.millicredits,
        ":title": sd.title,
        ":days": sd.days,
        ":time": sd.time,
        ":instructor": sd.instructor,
        ":date": sd.date,
        ":location": sd.location,
        ":status": sd.status,
        ":capacity": sd.capacity,
        ":actual": sd.actual,
        ":remaining": sd.remaining,
        ":wl_capacity": sd.wl_capacity,
        ":wl_actual": sd.wl_actual,
        ":wl_remaining": sd.wl_remaining,
        ":notes": notes,
    })
    .map(|_| ())
}

fn section_description_from_row(row: &Row) -> Result<String, rusqlite::Error> {
    Ok(format!(
        "{} {}-{}: {} ({}: {}){}",
        row.get::<_, Subject>(0)?,
        CourseNumber::from_row_indices(row, 1, 2)?,
        row.get_ref(3)?.as_str()?,
        row.get_ref(4)?.as_str()?,
        row.get_ref(5)?.as_str()?,
        row.get::<_, u32>(6)?,
        if row.get::<_, bool>(7)? {
            " _(All updates included)_"
        } else {
            ""
        }
    ))
}

#[allow(clippy::too_many_arguments)]
pub fn lookup_latest_sections_with_term_name(
    conn: &Connection,
    term: Option<u32>,
    crn: Option<u32>,
    subject: Option<Subject>,
    number: Option<CourseNumber>,
    section: Option<String>,
    limit: usize,
    all_spans: bool,
) -> Result<Vec<(SectionRecord, String)>, rusqlite::Error> {
    let (number, span) = number
        .as_ref()
        .map(|n| (n.number_tosql(), n.span_tosql(all_spans)))
        .unzip();

    conn.prepare_cached(
        "SELECT sections.*, terms.name \
            FROM sections INDEXED BY latest_sections_crn JOIN terms ON term = id  \
            WHERE latest = 1 AND (?1 IS NULL OR term = ?1) AND (?2 IS NULL OR crn = ?2) AND \
            (?3 IS NULL OR subject = ?3) AND (?4 IS NULL OR number = ?4) AND \
            (?5 IS NULL OR span = ?5) AND (?6 IS NULL OR section = ?6) \
            ORDER BY subject, number, span, term, section LIMIT ?7;",
    )?
    .query_map((term, crn, subject, number, span, section, limit), |row| {
        SectionRecord::try_from(row).and_then(|sr| row.get(25).map(|tn| (sr, tn)))
    })?
    .collect()
}

// Subscriptions

/// List of channels and whether they want all updates or only availabilities for a given course
pub fn subscriptions_to_section(
    trx: &Transaction,
    term: u32,
    crn: u32,
) -> Result<Vec<(u64, bool)>, rusqlite::Error> {
    trx.prepare_cached(
        "SELECT channel, all_updates FROM subscriptions WHERE term = ?1 AND crn = ?2;",
    )?
    .query((term, crn))?
    .map(|r| Ok((r.get::<_, u64>(0)?, r.get::<_, bool>(1)?)))
    .collect()
}

pub fn subscriptions_for_channel(
    trx: &Transaction,
    channel: u64,
) -> Result<Vec<(u32, u32, bool)>, rusqlite::Error> {
    trx.prepare_cached("SELECT (term, crn, all_updates) FROM subscriptions WHERE channel = ?1;")?
        .query([channel])?
        .map(|row| {
            Ok((
                row.get::<_, u32>(0)?,
                row.get::<_, u32>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })
        .collect()
}

pub fn subscriptions_term_crn_all(
    conn: &Connection,
) -> Result<HashMap<u32, Vec<u32>>, rusqlite::Error> {
    conn.prepare_cached("SELECT DISTINCT term, crn FROM subscriptions;")?
        .query(())?
        .map(|row| Ok((row.get::<_, u32>(0)?, row.get::<_, u32>(1)?)))
        .fold(HashMap::new(), |mut map, (t, c)| {
            map.entry(t).and_modify(|v| v.push(c)).or_insert(vec![c]);
            Ok(map)
        })
}

pub fn subscriptions_with_latest_description_for_channel(
    conn: &Connection,
    channel: u64,
    page: u64,
) -> Result<(u64, Vec<String>), rusqlite::Error> {
    let pages = conn
        .prepare("SELECT count(*) FROM subscriptions;")?
        .query_one((), |row| row.get::<_, u64>(0))?
        .div_ceil(24);
    let page = conn
        .prepare_cached(
            "SELECT sec.subject, sec.number, sec.span, sec.section, sec.title, terms.name, sec.crn, sub.all_updates \
            FROM subscriptions sub \
            JOIN sections sec INDEXED BY latest_sections_crn \
            ON latest = 1 AND sub.term = sec.term AND sub.crn = sec.crn \
            JOIN terms ON sub.term = id \
            WHERE channel = ?1 \
            ORDER BY sec.subject, sec.number, sec.span, sec.term, sec.section \
            LIMIT 24 OFFSET ?2;",
        )?
        .query_map((channel, (page - 1) * 24), section_description_from_row)?
        .collect::<Result<Vec<String>, rusqlite::Error>>()?;
    Ok((pages, page))
}

pub fn subscribe_channel_to_section_by_crn(
    conn: &Connection,
    channel: u64,
    term: u32,
    crn: u32,
    all_updates: bool,
) -> Result<(), rusqlite::Error> {
    if conn.prepare("SELECT NULL FROM subscriptions WHERE channel = ?1 AND term = ?2 AND crn = ?3 AND all_updates = ?4;")?.query_one((channel, term, crn, all_updates), |_| Ok(())).optional()?.is_some() {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                extended_code: rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
            },
            None,
        ));
    }
    conn.prepare("INSERT OR REPLACE INTO subscriptions VALUES (?1, ?2, ?3, ?4);")?
        .execute((channel, term, crn, all_updates))
        .and_then(|res| match res {
            1 => Ok(()),
            o => Err(rusqlite::Error::StatementChangedRows(o)),
        })
}

pub fn unsubscribe_channel_from_section_by_crn(
    conn: &Connection,
    channel: u64,
    term: u32,
    crn: u32,
) -> Result<(), rusqlite::Error> {
    conn.prepare("DELETE FROM subscriptions WHERE channel = ?1 AND term = ?2 AND crn = ?3;")?
        .execute((channel, term, crn))
        .and_then(|num| match num {
            1 => Ok(()),
            0 => Err(rusqlite::Error::QueryReturnedNoRows),
            _ => Err(rusqlite::Error::QueryReturnedMoreThanOneRow),
        })
}

#[allow(clippy::too_many_arguments)]
pub fn subscribe_channel_to_section_by_subj_number(
    conn: &Connection,
    channel: u64,
    term: u32,
    subject: Subject,
    number: CourseNumber,
    section: Option<String>,
    all_spans: bool,
    all_updates: bool,
) -> Result<usize, rusqlite::Error> {
    let num_sql = number.number_tosql();
    let span_sql = number.span_tosql(all_spans);

    conn.prepare(
        "INSERT OR REPLACE INTO subscriptions SELECT ?1, term, crn, ?7 \
        FROM sections INDEXED BY latest_sections_crn WHERE latest = 1 AND term = ?2 AND \
        subject = ?3 AND number = ?4 AND (?5 IS NULL OR span = ?5) AND \
        (?6 IS NULL OR section = ?6)",
    )?
    .execute((
        channel,
        term,
        subject,
        num_sql,
        span_sql,
        section,
        all_updates,
    ))
}

pub fn unsubscribe_channel_to_section_by_subj_number(
    conn: &Connection,
    channel: u64,
    term: u32,
    subject: Subject,
    number: CourseNumber,
    section: Option<String>,
    all_spans: bool,
) -> Result<usize, rusqlite::Error> {
    let num_sql = number.number_tosql();
    let span_sql = number.span_tosql(all_spans);

    conn.prepare(
        "DELETE FROM subscriptions WHERE channel = ?1 AND (subscriptions.term, subscriptions.crn) \
            IN (SELECT term, crn FROM sections INDEXED BY latest_sections_crn WHERE latest = 1 AND \
            term = ?2 AND subject = ?3 AND number = ?4 AND (?5 IS NULL OR span = ?5) AND \
            (?6 IS NULL OR section = ?6))",
    )?
    .execute((channel, term, subject, num_sql, span_sql, section))
}

pub fn unsubscribe_channel_from_all(
    conn: &Connection,
    channel: u64,
) -> Result<usize, rusqlite::Error> {
    conn.prepare("DELETE FROM subscriptions WHERE channel = ?1;")?
        .execute((channel,))
}

// Terms

pub fn parse_term(conn: &Connection, term: &str) -> Result<Option<u32>, rusqlite::Error> {
    if let Ok(val) = term.parse::<u32>() {
        conn.prepare_cached("SELECT id FROM terms WHERE id = ?1;")?
            .query_one((val,), |r| r.get::<_, u32>(0))
            .optional()
    } else {
        conn.prepare_cached("SELECT id FROM terms WHERE name = ?1;")?
            .query_one((term,), |r| r.get::<_, u32>(0))
            .optional()
    }
}

pub fn term_ids(conn: &Connection, minimum: u32) -> Result<Vec<u32>, rusqlite::Error> {
    conn.prepare("SELECT id FROM terms WHERE id >= ?1;")?
        .query([minimum])?
        .and_then(|r| r.get::<_, u32>(0))
        .collect::<Result<Vec<u32>, _>>()
}

pub fn insert_terms_from_map(
    conn: &mut Connection,
    term_map: &HashMap<u32, String>,
) -> Result<(), rusqlite::Error> {
    let trx = conn.transaction()?;

    {
        let mut statement = trx.prepare("INSERT OR REPLACE INTO terms VALUES (?1, ?2);")?;
        for (id, name) in term_map {
            statement.execute((id, name))?;
        }
    }

    trx.commit()?;
    Ok(())
}
