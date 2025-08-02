use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Write;
use std::iter;
use std::num::ParseIntError;
use std::thread::scope;

use chrono::{DateTime, Utc};

use reqwest::blocking::Client;

use roxmltree::{Document, Node};

use crate::db::SectionRecord;
use crate::minerva::parser::Status;
use thiserror::Error;

const BASE_URL: &str = "https://vsb.mcgill.ca/api/";

// Just get the basic numbers and avoid string processing
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct VsbRecord {
    pub timestamp: DateTime<Utc>,
    pub term: u32,
    pub crn: u32,
    pub status: Status,
    pub remaining: i16,
    pub wl_capacity: i16,
    pub wl_remaining: i16,
}

#[derive(Debug, Error)]
pub enum VsbParserError {
    #[error("Missing required attribute '{0}' on section block element")]
    MissingAttr(&'static str),
    #[error("Attribute {0} had unexpected value '{1}'")]
    UnexpectedValue(&'static str, String),
    #[error("Failed to parse attribute value as integer: {0}")]
    ParseIntError(ParseIntError),
    #[error("Different results were returned for the same section {0}")]
    ConflictingResults(u32),
    #[error("Attempted to apply VSB data to non-matching section")]
    NonMatchingData,
}

impl From<ParseIntError> for VsbParserError {
    fn from(value: ParseIntError) -> Self {
        Self::ParseIntError(value)
    }
}

impl VsbRecord {
    fn parse(timestamp: DateTime<Utc>, term: u32, node: Node) -> Result<Self, VsbParserError> {
        let status_s = node
            .attribute("status")
            .ok_or(VsbParserError::MissingAttr("status"))?;
        let status = match status_s {
            "A" => Status::Active,
            "T" => Status::TemporarilyClosed,
            "R" => Status::RegistrationNotRequired,
            o => return Err(VsbParserError::UnexpectedValue("status", o.to_string())),
        };

        Ok(Self {
            term,
            timestamp,
            crn: node
                .attribute("key")
                .ok_or(VsbParserError::MissingAttr("key"))?
                .parse()?,
            status,
            remaining: node
                .attribute("os")
                .ok_or(VsbParserError::MissingAttr("os"))?
                .parse()?,
            wl_remaining: node
                .attribute("ws")
                .ok_or(VsbParserError::MissingAttr("ws"))?
                .parse()?,
            wl_capacity: node
                .attribute("wc")
                .ok_or(VsbParserError::MissingAttr("wc"))?
                .parse()?,
        })
    }

    pub fn merge_with_full(
        self,
        mut other: SectionRecord,
    ) -> Result<SectionRecord, VsbParserError> {
        if !(other.term == self.term && other.inner.crn == self.crn) {
            return Err(VsbParserError::NonMatchingData);
        }

        other.timestamp = self.timestamp;
        other.inner.status = self.status;
        other.inner.remaining = self.remaining;
        other.inner.actual = other.inner.capacity - self.remaining;
        other.inner.wl_capacity = self.wl_capacity;
        other.inner.wl_remaining = self.wl_remaining;
        other.inner.wl_actual = self.wl_capacity - self.wl_remaining;

        Ok(other)
    }
}

// block tag attrs (see https://vsb.mcgill.ca/js/legend.js?v=3030 and https://vsb.mcgill.ca/js/unity.js?v=3030)
// type: str section type (needs transform to match minerva, Lec = Lecture, Tut = Tutorial, etc.)  -> sec_type
// key, cartid: int crn  -> crn
// secNo: int section  -> section
// status: str status (A = active, T = temp closed, R = no reg needed)  -> status
// u: bool seat availability unknown  -> skip if set
// os: int open seats, 9999 => unlimited  -> rem
// me: int class cap but broken?
// csos: int combined section open seats
// csme: int combined section cap if != -1
// isFull: os == 0
// ws: waitlist remaining  -> wl_rem
// wc: waitlist capacity  -> wl_cap
// custstat: str custom seat status, O=open, F=full, X=cancelled, C=closed  -> status
// nres: non reserved seats
// hue: hidden unless enrolled
// c: closed  -> status
// n: note  -> notes
// txtb: textbooks?
// fd: full description
// disp: not needed, section name
// usn: str u? section name??
// ot: str offline/offline? usually c
// recl: "Number of students who have a recommendation locking them into this very class"
// recul: "Number of students who have a recommendation on this course"
// credits: str creds  -> millicredits
// creditsMax: str max creds
// rgs, ac, pn, im, hs: ?
// attrs, eattrs: attributes?
// teacher, location: not set when not signed in
// campus: str
// psl: ?
// timeblockids: ignore
// loos, loot: ?

// From https://vsb.mcgill.ca/js/common.js?v=3030
// ```js
// function nWindow() {
// 	var f8b0=["\x26\x74\x3D","\x26\x65\x3D"]
// 	var t=(Math.floor((new Date())/60000))%1000;
// 	var e=t%3+t%39+t%42;
// 	return f8b0[0]+t+f8b0[1]+e;
// }
// ```
fn time_params() -> (DateTime<Utc>, u16, u8) {
    let now = Utc::now();
    let ts = now.timestamp();
    let t = (ts / 60) % 1000;
    let e = (t % 3) + (t % 39) + (t % 42);
    (now, t as u16, e as u8)
}

/// VSB seems to limit responses to <455 sections, no matter their actual response size
pub fn get(client: &Client, term: u32, crns: &[u32]) -> Result<Vec<VsbRecord>, anyhow::Error> {
    let (timestamp, t, e) = time_params();
    // VSB web app uses SUBJ-NUM format in query params, but crn also just works...
    let url = crns.iter().enumerate().fold(
        format!("{BASE_URL}class-data?term={term}&t={t}&e={e}"),
        |mut acc, (i, crn)| {
            write!(acc, "&course_{i}_0={crn}").unwrap();
            acc
        },
    );
    let resp = client.get(url).send()?.error_for_status()?.text()?;

    let doc = Document::parse(&resp)?;
    let blocks = doc.descendants().filter(|e| e.has_tag_name("block"));

    let mut map = HashMap::new();
    for block in blocks {
        let data = VsbRecord::parse(timestamp, term, block)?;
        match map.entry(data.crn) {
            Entry::Occupied(e) => {
                if e.get() != &data {
                    return Err(VsbParserError::ConflictingResults(data.crn).into());
                }
            }
            Entry::Vacant(v) => {
                v.insert(data);
            }
        }
    }

    Ok(map.into_values().collect())
}

/// Races chunked requests of 128 courses to the VSB
pub fn vsb_get(
    cl: &Client,
    term_crns: &HashMap<u32, Vec<u32>>,
) -> Result<Vec<VsbRecord>, anyhow::Error> {
    const CHUNK_SIZE: usize = 128;

    let term_crns_chunked = term_crns
        .iter()
        .flat_map(|(t, crns)| crns.chunks(CHUNK_SIZE).map(|c| (*t, c)))
        .collect::<Vec<(u32, &[u32])>>();

    let mut thread_results = iter::repeat_with(|| None)
        .take(term_crns_chunked.len())
        .collect::<Vec<_>>();

    scope(|s| {
        let mut remaining_slice = thread_results.as_mut_slice();
        for (term, crns) in term_crns_chunked {
            let (out, rem) = remaining_slice.split_at_mut(1);
            remaining_slice = rem;
            s.spawn(move || out[0] = Some(get(cl, term, crns)));
        }
    });

    thread_results
        .into_iter()
        .flat_map(|opt| {
            let mut ok = None;
            let mut err = None;
            match opt.unwrap() {
                Ok(v) => ok = Some(v.into_iter().map(Ok)),
                Err(e) => err = Some(Err(e)),
            }
            ok.into_iter().flatten().chain(err.into_iter().flatten())
        })
        .collect()
}
