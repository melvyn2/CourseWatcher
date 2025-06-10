use std::array::TryFromSliceError;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::scraper::ScraperError;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Select {
    Open = 0,
    Closed = 1,
    Empty = 2,
    NotAvailable = 3,
    StudentRestriction = 4,
}

impl TryFrom<u8> for Select {
    type Error = ScraperError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Open),
            1 => Ok(Self::Closed),
            2 => Ok(Self::Empty),
            3 => Ok(Self::NotAvailable),
            4 => Ok(Self::StudentRestriction),
            v => Err(Self::Error::ParseError(format!(
                "No Select variant for integer {v}",
            ))),
        }
    }
}

impl TryFrom<&str> for Select {
    type Error = ScraperError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "add to worksheet" => Ok(Select::Open),
            "C" => Ok(Select::Closed),
            "" => Ok(Select::Empty),
            "NR" => Ok(Select::NotAvailable),
            "SR" => Ok(Select::StudentRestriction),
            v => Err(ScraperError::ParseError(format!(
                "Unknown select contents: {v}",
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Status {
    Active = 0,
    RegistrationNotRequired = 1,
    Cancelled = 2,
    TemporarilyClosed = 3,
}

impl TryFrom<u8> for Status {
    type Error = ScraperError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Active),
            1 => Ok(Self::RegistrationNotRequired),
            2 => Ok(Self::Cancelled),
            3 => Ok(Self::TemporarilyClosed),
            v => Err(Self::Error::ParseError(format!(
                "No Status variant for integer {v}",
            ))),
        }
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Active => write!(f, "Active"),
            Status::RegistrationNotRequired => write!(f, "No Registration"),
            Status::Cancelled => write!(f, "Cancelled"),
            Status::TemporarilyClosed => write!(f, "Temporarily Closed"),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Subject(pub [u8; 4]);
impl Display for Subject {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            str::from_utf8(&self.0).map_err(|_| std::fmt::Error)?
        )
    }
}

impl TryFrom<&str> for Subject {
    type Error = TryFromSliceError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        <[u8; 4]>::try_from(value.as_bytes()).map(Self)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum CourseNumber {
    Plain(u16),
    Spanned(u16, [u8; 2]),
    Other(String),
}

impl Display for CourseNumber {
    fn fmt(&self, fmtr: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CourseNumber::Plain(n) => write!(fmtr, "{n}"),
            CourseNumber::Spanned(n, s) => write!(fmtr, "{n}{}{}", s[0] as char, s[1] as char),
            CourseNumber::Other(s) => write!(fmtr, "{s}"),
        }
    }
}

impl TryFrom<String> for CourseNumber {
    type Error = ScraperError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.len() {
            3 if let Ok(num) = value.parse() => Ok(CourseNumber::Plain(num)),
            // Length is already checked, force
            5 if let Ok(num) = value[..3].parse() => Ok(CourseNumber::Spanned(
                num,
                value.as_bytes()[3..].try_into().unwrap(),
            )),
            0 => Err(Self::Error::ParseError(
                "Unexpected empty course number".to_string(),
            )),
            _ => Ok(CourseNumber::Other(value)),
        }
    }
}

// impl From<String> for CourseNumber {
//     fn from(value: String) -> Self {
//         match value.len() {
//             3 if let Ok(num) = value.parse() => CourseNumber::Plain(num),
//             // Length is already checked, force
//             5 if let Ok(num) = value[..3].parse() => {
//                 CourseNumber::Spanned(num, value.as_bytes()[3..].try_into().unwrap())
//             }
//             _ => CourseNumber::Other(value),
//         }
//     }
// }

impl Ord for CourseNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (CourseNumber::Plain(ln), CourseNumber::Plain(rn)) => ln.cmp(rn),
            (CourseNumber::Plain(ln), CourseNumber::Spanned(rn, _)) => {
                ln.cmp(rn).then(Ordering::Less)
            }
            (CourseNumber::Spanned(ln, _), CourseNumber::Plain(rn)) => {
                ln.cmp(rn).then(Ordering::Greater)
            }
            (CourseNumber::Spanned(ln, ls), CourseNumber::Spanned(rn, rs)) => {
                ln.cmp(rn).then_with(|| ls.cmp(rs))
            }
            (CourseNumber::Other(lo), CourseNumber::Other(ro)) => lo.cmp(ro),
            (CourseNumber::Other(_), _) => Ordering::Greater,
            (_, CourseNumber::Other(_)) => Ordering::Less,
        }
    }
}

impl PartialOrd for CourseNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum SectionNumber {
    Plain(u16),
    Other(String),
}

impl Display for SectionNumber {
    fn fmt(&self, fmtr: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SectionNumber::Plain(n) => write!(fmtr, "{n:03}"),
            SectionNumber::Other(s) => write!(fmtr, "{s}"),
        }
    }
}

impl TryFrom<String> for SectionNumber {
    type Error = ScraperError;

    fn try_from(section_s: String) -> Result<Self, Self::Error> {
        if section_s.is_empty() {
            return Err(ScraperError::ParseError(
                "Unexpected empty section number".to_string(),
            ));
        }
        Ok(match section_s.parse() {
            Ok(n) => SectionNumber::Plain(n),
            Err(_) => SectionNumber::Other(section_s),
        })
    }
}

impl Ord for SectionNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (SectionNumber::Plain(ln), SectionNumber::Plain(rn)) => ln.cmp(rn),
            (SectionNumber::Other(lo), SectionNumber::Other(ro)) => lo.cmp(ro),
            (SectionNumber::Other(_), SectionNumber::Plain(_)) => Ordering::Greater,
            (SectionNumber::Plain(_), SectionNumber::Other(_)) => Ordering::Less,
        }
    }
}

impl PartialOrd for SectionNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Milliunits(pub u16);
impl Display for Milliunits {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.0 / 1000, self.0 % 1000)
    }
}

impl TryFrom<&str> for Milliunits {
    type Error = ScraperError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value
            .split_once('.')
            .and_then(|(d_s, m_s)| {
                if m_s.len() == 3 {
                    d_s.parse::<u16>()
                        .ok()
                        .zip_with(m_s.parse::<u16>().ok(), |d, m| (d * 1000) + m)
                        .map(Milliunits)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                ScraperError::ParseError(format!("Could not parse the credit-count cell: {value}"))
            })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Days(pub Option<[bool; 7]>);
impl Display for Days {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(days) = self.0 {
            if days[0] {
                write!(f, "M")?;
            }
            if days[1] {
                write!(f, "T")?;
            }
            if days[2] {
                write!(f, "W")?;
            }
            if days[3] {
                write!(f, "R")?;
            }
            if days[4] {
                write!(f, "F")?;
            }
            if days[5] {
                write!(f, "S")?;
            }
            if days[6] {
                write!(f, "U")?;
            }
            Ok(())
        } else {
            write!(f, "TBA")
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Availability {
    Available,
    Full,
    // Registered,
    WaitlistAvailable,
    WaitlistFull,
    Cancelled,
    TemporarilyClosed,
    NotYetOpen,
    RegistrationNotRequired,
    // Hold,
}

impl Display for Availability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Availability::Available => "Seats may be available",
            Availability::Full => "Section is full",
            Availability::WaitlistAvailable => "Waitlist slots may be available",
            Availability::WaitlistFull => "Waitlist is full",
            Availability::Cancelled => "Section has been cancelled",
            Availability::TemporarilyClosed => "Section is temporarily closed",
            Availability::NotYetOpen => "Section is not yet open for registration (NR)",
            Availability::RegistrationNotRequired => "Section does not require registration",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SectionData {
    pub select: Select,
    pub crn: u32,
    pub subject: Subject,
    // pub number: u16,
    // pub span: Option<[u8; 2]>,
    pub number: CourseNumber,
    pub section: SectionNumber,
    pub sec_type: String,
    pub millicredits: Milliunits,
    pub title: String,
    pub days: Days,
    pub time: Option<String>,
    // TODO support extra time rows
    // pub times: Vec<SectionTimes>,
    pub capacity: i16,
    pub actual: i16,
    pub remaining: i16,
    pub wl_capacity: i16,
    pub wl_actual: i16,
    pub wl_remaining: i16,
    pub instructor: Option<String>,
    pub date: String,
    pub location: Option<String>,
    pub status: Status,
    // term: u32,
    pub notes: Vec<String>,
}

impl SectionData {
    fn parse_times_from_days_time(days_s: &str, time_s: &str) -> Result<Vec<String>, ScraperError> {
        if days_s == "TBA" {
            Ok(vec![])
        } else {
            let time = if time_s.is_empty() { "TBA" } else { time_s };

            days_s
                .chars()
                .map(|c| {
                    let day_name = match c {
                        'M' => "Monday",
                        'T' => "Tuesday",
                        'W' => "Wednesday",
                        'R' => "Thursday",
                        'F' => "Friday",
                        'S' => "Saturday",
                        'U' => "Sunday",
                        o => {
                            return Err(ScraperError::ParseError(format!(
                                "Unknown day abbreviation: '{o}'"
                            )));
                        }
                    };
                    Ok(format!("{day_name} {time}"))
                })
                .collect::<Result<Vec<String>, ScraperError>>()
        }
    }

    pub fn parse(columns: [String; 20] /* , term: u32 */) -> Result<Self, anyhow::Error> {
        // log::trace!("Parsing section from columns: {columns:?}");

        let [
            select_s,
            crn_s,
            subj_s,
            num_s,
            section_s,
            sec_type_s,
            credits_s,
            mut title_s,
            days_s,
            time_s,
            capacity_s,
            actual_s,
            remaining_s,
            wl_capacity_s,
            wl_actual_s,
            wl_remaining_s,
            instructor_s,
            date_s,
            location_s,
            status_s,
        ] = columns;

        // let select = match select_s.as_str() {
        //     "add to worksheet" => Select::Open,
        //     "C" => Select::Closed,
        //     "" => Select::Empty,
        //     "NR" => Select::NotAvailable,
        //     "SR" => Select::StudentRestriction,
        //     _ => {
        //         return Err(ScraperError::ParseError(format!(
        //             "Unknown select contents: {}",
        //             select_s
        //         ))
        //         .into());
        //     }
        // };

        let subject = subj_s
            .as_str()
            .try_into()
            .with_context(|| format!("Subject code not 4 bytes: {subj_s}"))?;

        // D1/D2 etc.
        // let span = {
        //     if num_s.len() > 3 {
        //         Some(
        //             num_s.as_bytes()[3..]
        //                 .try_into()
        //                 .with_context(|| num_s.clone())?,
        //         )
        //     } else {
        //         None
        //     }
        // };

        // fixed-point conversion
        let millicredits = credits_s
            .split_once('/')
            .map(|(l, _r)| l)
            .unwrap_or(&credits_s)
            .try_into()?;

        if title_s.ends_with('.') {
            title_s.remove(title_s.len() - 1);
        }

        let days = if days_s.is_empty() || days_s == "TBA" {
            Days(None)
        } else {
            let mut days = [false; 7];
            for c in days_s.chars() {
                let idx = match c {
                    'M' => 0,
                    'T' => 1,
                    'W' => 2,
                    'R' => 3,
                    'F' => 4,
                    'S' => 5,
                    'U' => 6,
                    // _ => return Err(ScraperError::InvalidDay(c).into()),
                    o => {
                        return Err(ScraperError::ParseError(format!(
                            "Unknown day abbreviation: '{o}'"
                        ))
                        .into());
                    }
                };
                days[idx] = true;
            }
            Days(Some(days))
        };

        let time = if time_s.is_empty() || time_s == "TBA" {
            None
        } else {
            Some(time_s)
        };

        let instructor = if instructor_s.is_empty() || instructor_s == "TBA" {
            None
        } else {
            Some(instructor_s.replace("  ", " ").replace(" ,", ","))
        };

        let location = if location_s.is_empty() || location_s == "TBA" {
            None
        } else {
            Some(location_s)
        };

        let status = match status_s.as_str() {
            "Active" => Status::Active,
            "Cancelled" => Status::Cancelled,
            "Temporarily closed" => Status::TemporarilyClosed,
            "Registration Not Required" => Status::RegistrationNotRequired,
            s => {
                return Err(
                    ScraperError::ParseError(format!("Unknown section status: '{s}'")).into(),
                );
            }
        };

        Ok(Self {
            select: select_s.as_str().try_into()?,
            crn: crn_s.parse()?,
            subject,
            number: num_s.try_into()?,
            section: section_s.try_into()?,
            sec_type: sec_type_s,
            millicredits,
            title: title_s,
            days,
            time,
            capacity: capacity_s.parse().with_context(|| capacity_s)?,
            actual: actual_s.parse()?,
            remaining: remaining_s.parse()?,
            wl_capacity: wl_capacity_s.parse()?,
            wl_actual: wl_actual_s.parse()?,
            wl_remaining: wl_remaining_s.parse()?,
            instructor,
            date: date_s,
            location,
            status,
            // term,
            notes: vec![],
        })
    }
    pub fn add_note(&mut self, note: &str) {
        self.notes.push(note.to_string())
    }

    pub fn availability(&self) -> Availability {
        match (
            self.select,
            self.status,
            self.remaining,
            self.wl_actual,
            self.wl_remaining,
        ) {
            (Select::NotAvailable, _, _, _, _) => Availability::NotYetOpen,
            (_, Status::Active, ..=0, ..=0, ..=0) => Availability::Full,
            (_, Status::Active, 1.., ..=0, _) => Availability::Available,
            (_, Status::Active, _, 1.., ..=0) => Availability::WaitlistFull,
            (_, Status::Active, _, _, 1..) => Availability::WaitlistAvailable,
            (_, Status::Cancelled, _, _, _) => Availability::Cancelled,
            (_, Status::TemporarilyClosed, _, _, _) => Availability::TemporarilyClosed,
            (_, Status::RegistrationNotRequired, _, _, _) => Availability::RegistrationNotRequired,
        }
    }
}
