use std::collections::HashMap;
use std::fmt::Debug;
use std::num::ParseIntError;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;

use anyhow::Context;

use reqwest::Url;
use reqwest::blocking::{Client, ClientBuilder, Request, Response};

use scraper::{Html, Selector};

use parser::SectionData;

pub mod parser;

const BASE_URL: &str = "https://horizon.mcgill.ca/pban1/";

#[derive(Debug, Error)]
pub enum MinervaScraperError {
    #[error("Failed to log in: Authorization Failure")]
    LoginFailure,
    #[error("Not currently logged in")]
    LoggedOut,
    // #[error("No such section was found in the given course")]
    // NoSection,
    #[error("Failed to parse the results table: {0}")]
    ParseError(String),
}

pub fn new_client() -> Result<Client, anyhow::Error> {
    Ok(ClientBuilder::new()
        .https_only(true)
        .cookie_store(true)
        .user_agent(format!(
            "{} / {}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
        ))
        .build()?)
}

fn abs_url(rel_path: &str) -> Result<Url, anyhow::Error> {
    Ok(Url::from_str(BASE_URL).and_then(|u| u.join(rel_path))?)
}

fn send_req(cl: &Client, req: Request) -> Result<Response, anyhow::Error> {
    cl.execute(req)?.error_for_status().map_err(|e| e.into())
}

fn quick_get(cl: &Client, rel_path: &str) -> Result<Response, anyhow::Error> {
    let req = cl.get(abs_url(rel_path)?).build()?;

    send_req(cl, req)
}

fn quick_post(cl: &Client, rel_path: &str, data: String) -> Result<Response, anyhow::Error> {
    let req = cl
        .post(abs_url(rel_path)?)
        .body(data)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .build()?;

    send_req(cl, req)
}

pub fn check_login(cl: &Client) -> Result<(), anyhow::Error> {
    if quick_get(cl, "twbkwbis.P_GenMenu?name=bmenu.P_MainMnu")?
        .text()?
        .contains("/pban1/twbkwbis.P_ValLogin")
    {
        log::info!("Session is no longer valid");
        Err(MinervaScraperError::LoggedOut.into())
    } else {
        log::debug!("Session is still valid");
        Ok(())
    }
}

pub fn login(cl: &Client, uid: &str, pin: &str) -> Result<String, anyhow::Error> {
    log::trace!("Attempting UID/PIN login");

    // Necessary for cookie check
    quick_get(cl, "twbkwbis.P_WWWLogin")?;

    let resp = quick_post(cl, "twbkwbis.P_ValLogin", format!("sid={uid}&PIN={pin}"))?;

    if let Some(sessid) = resp.cookies().find(|c| c.name() == "SESSID")
        && !sessid.value().is_empty()
    {
        quick_get(cl, "twbkwbis.P_GenMenu?name=bmenu.P_MainMnu")?;

        log::debug!("Authenticated using UID/PIN");

        Ok(sessid.value().to_string())
    } else {
        Err(MinervaScraperError::LoginFailure.into())
    }
}

pub fn get_term_map(cl: &Client, include_ro: bool) -> Result<HashMap<u32, String>, anyhow::Error> {
    log::trace!(
        "Fetching {} terms",
        if include_ro {
            "all"
        } else {
            "currently active"
        }
    );

    let resp = quick_get(cl, "bwskfcls.p_sel_crse_search")?;
    let text = resp.text()?;

    // fix page template typo
    let text = text.replacen("BYPASS_ESC=>", "BYPASS_ESC=", 1);

    log::trace!("Parsing {} byte term selection document", text.len());

    // println!("{}", text);

    // The page has a typo that prevents the id attr from being parsed, otherwise
    // `#term_input_id > option` would be used
    // Constant argument is ok to unwrap
    let selector = Selector::parse("select[name=\"p_term\"] > option").unwrap();
    let html = Html::parse_document(&text);

    let options = html.select(&selector);

    let terms = options
        .map(|e| (e.attr("value").unwrap_or("").to_string(), e.inner_html()))
        .filter(|(id, _n)| !id.is_empty())
        .filter(|(_id, name)| include_ro || !name.ends_with("(View only)"))
        .map(|(id, name)| {
            id.parse()
                .map(|k| (k, name))
                .map_err(|e: ParseIntError| e.into())
        })
        .collect::<Result<HashMap<u32, String>, anyhow::Error>>();

    log::debug!(
        "Found {} terms",
        terms.as_ref().map(|v| v.len()).unwrap_or(0)
    );

    terms
}

pub fn get_subj_fac_codes_for_term(
    cl: &Client,
    term: u32,
) -> Result<(Vec<String>, Vec<String>), anyhow::Error> {
    log::trace!("Fetching subjects and faculties during {term}");

    let form_data = format!(
        "term_in={term}&\
            rsts=dummy&crn=dummy&sel_subj=dummy&sel_day=dummy&sel_schd=dummy&sel_insm=dummy&\
            sel_camp=dummy&sel_levl=dummy&sel_sess=dummy&sel_instr=dummy&sel_ptrm=dummy&\
            sel_attr=dummy&sel_crse=&sel_title=&sel_from_cred=&sel_to_cred=&sel_ptrm=%25&\
            begin_hh=0&begin_mi=0&end_hh=0&end_mi=0&begin_ap=x&end_ap=y&path=1&\
            SUB_BTN=Advanced+Search"
    );

    let resp = quick_post(cl, "bwskfcls.P_GetCrse", form_data)?;
    let text = resp.text()?;

    log::trace!(
        "Parsing {} byte subject and faculty selection document",
        text.len()
    );

    let subj_selector = Selector::parse("#subj_id > OPTION").unwrap();
    // ...and this select element has no id attr at all
    let school_selector = Selector::parse("select[name=\"sel_coll\"] > OPTION").unwrap();
    let html = Html::parse_document(&text);

    let subj_ids = html
        .select(&subj_selector)
        .filter_map(|e| e.attr("value").map(|v| v.to_string()))
        .collect::<Vec<String>>();
    let fac_ids = html
        .select(&school_selector)
        .filter_map(|e| e.attr("value").map(|v| v.to_string()))
        .collect::<Vec<String>>();

    log::debug!(
        "Found {} subjects and {} faculties in {}",
        subj_ids.len(),
        fac_ids.len(),
        term
    );

    if subj_ids.len() == 0 {
        static FILE_COUNT: AtomicUsize = AtomicUsize::new(0);
        std::fs::write(
            format!("dump{}.html", FILE_COUNT.fetch_add(1, Ordering::SeqCst)),
            text,
        )
        .unwrap();
    }

    Ok((subj_ids, fac_ids))
}

pub fn get_sections_for_term_subjs_fac(
    cl: &Client,
    term: u32,
    subj_ids: &[String],
    fac_id: Option<&str>,
    skip_error_rows: bool,
) -> Result<Vec<SectionData>, anyhow::Error> {
    log::trace!(
        "Fetching sections for {} subjects{} during {}",
        subj_ids.len(),
        fac_id.map(|f| format!(" in {f}")).unwrap_or(String::new()),
        term
    );

    let form_data = format!(
        "rsts=dummy&crn=dummy&sel_subj=dummy&\
                sel_subj={}&term_in={term}{}&\
                sel_day=dummy&sel_schd=dummy&sel_insm=dummy&\
                sel_camp=dummy&sel_levl=dummy&sel_sess=dummy&sel_instr=dummy&sel_ptrm=dummy&\
                sel_attr=dummy&\
                sel_crse=&sel_title=&sel_schd=%25&sel_from_cred=&sel_to_cred=&sel_levl=%25&\
                sel_ptrm=%25&sel_instr=%25&sel_attr=%25&begin_hh=0&begin_mi=0&begin_ap=a&end_hh=0&\
                end_mi=0&end_ap=a&SUB_BTN=Get+Course+Sections&path=1",
        subj_ids.join("&sel_subj="),
        fac_id
            .map(|f| format!("&sel_coll={f}"))
            .unwrap_or(String::new())
    );

    let resp = quick_post(cl, "bwskfcls.P_GetCrse_Advanced", form_data)?;
    let text = resp.text()?;

    parse_results_table(&text, skip_error_rows).with_context(|| {
        format!(
            "Error parsing courses{} during {term} for {} subjects",
            fac_id.map(|f| format!(" in {f}")).unwrap_or(String::new()),
            subj_ids.len()
        )
    })
}

// pub fn get_sections_for_term_subj_number(
//     cl: &Client,
//     term: u32,
//     subject: &str,
//     number: &CourseNumber,
//     skip_error_rows: bool,
// ) -> Result<Vec<SectionData>, anyhow::Error> {
//     log::trace!("Fetching sections for course {subject} {number} during {term}");
//
//     let form_data = format!(
//         "term_in={term}&\
//             sel_subj=dummy&\
//             sel_subj={subject}&\
//             SEL_CRSE={number}&\
//             SEL_TITLE=&BEGIN_HH=0&BEGIN_MI=0&BEGIN_AP=a&SEL_DAY=dummy&SEL_PTRM=dummy&END_HH=0&\
//             END_MI=0&END_AP=a&SEL_CAMP=dummy&SEL_SCHD=dummy&SEL_SESS=dummy&SEL_INSTR=dummy&\
//             SEL_INSTR=%25&SEL_ATTR=dummy&SEL_ATTR=%25&SEL_LEVL=dummy&SEL_LEVL=%25&SEL_INSM=dummy&\
//             sel_dunt_code=&sel_dunt_unit=&call_value_in=&rsts=dummy&path=1&SUB_BTN=View+Sections",
//     );
//
//     let resp = quick_post(cl, "bwskfcls.P_GetCrse", form_data)?;
//     let text = resp.text()?;
//
//     let sections = parse_results_table(&text, skip_error_rows)?;
//
//     Ok(sections)
// }

fn parse_results_table(
    text: &str,
    skip_error_rows: bool,
) -> Result<Vec<SectionData>, anyhow::Error> {
    log::trace!("Parsing {} byte section result document", text.len());

    if text.contains("No classes were found that meet your search criteria") {
        // return Err(ScraperError::NoCourseResults.into());
        return Ok(vec![]);
    }

    // Unwrap is fine since this is a constant anyway
    let selector = Selector::parse(".datadisplaytable > tbody > tr").unwrap();

    let doc = Html::parse_document(text);
    let mut table_rows = doc.select(&selector);

    // Consume the header rows
    let _title_row = table_rows
        .next()
        .ok_or_else(|| MinervaScraperError::ParseError("Missing title row".to_string()))?;
    let key_row = table_rows
        .next()
        .ok_or_else(|| MinervaScraperError::ParseError("Missing key row".to_string()))?;

    // to ensure the format hasn't changed unexpectedly
    let col_count = key_row.child_elements().count();
    if col_count != 20 {
        return Err(MinervaScraperError::ParseError(format!(
            "Unexpected column count: got {col_count}, expected 20"
        ))
        .into());
    }

    let mut data_rows = vec![];

    for row in table_rows {
        // Skip extra headers
        if row
            .child_elements()
            .next()
            .map(|e| e.value().name() == "th")
            .unwrap_or(true)
        {
            continue;
        }

        let children = row
            .child_elements()
            .flat_map(|e| {
                let text = e.text().fold(String::new(), |mut a, s| {
                    a.push_str(s.trim());
                    a
                });
                std::iter::once(text)
                    .chain(std::iter::repeat_with(String::new))
                    .take(
                        e.attr("colspan")
                            .and_then(|cs| cs.parse().ok())
                            .unwrap_or(1),
                    )
            })
            .collect::<Vec<String>>();

        if children.len() == 20 {
            // len has been checked, cannot fail
            let ch_array: [String; 20] = children.try_into().unwrap();
            if !ch_array[1].is_empty() {
                match SectionData::parse(ch_array) {
                    Ok(sd) => data_rows.push(sd),
                    Err(e) if skip_error_rows => log::warn!("Ignoring row error: {e:?}"),
                    Err(e) => return Err(e),
                }
            } else if !ch_array[8].is_empty() || !ch_array[9].is_empty() {
                // log::trace!("Skipping supplementary time row (TODO)")
            } else {
                if skip_error_rows {
                    log::warn!("Ignoring unknown 20-cell row format: {ch_array:?}");
                } else {
                    return Err(MinervaScraperError::ParseError(format!(
                        "Unknown 20-cell row format: {ch_array:?}"
                    ))
                    .into());
                }
            }
        } else if children.len() == 26 {
            // note for previous section
            for note in row.text().map(|t| t.trim()).filter(|t| !t.is_empty()) {
                match data_rows.last_mut() {
                    Some(sd) => sd.add_note(note),
                    None if skip_error_rows => {
                        log::warn!("Ignoring note before data row: {note}")
                    }
                    None => {
                        return Err(MinervaScraperError::ParseError(format!(
                            "Found note row before any sections: {note}"
                        ))
                        .into());
                    }
                }
            }
        } else {
            if skip_error_rows {
                log::warn!("Ignoring unexpected row with {} children", children.len());
            } else {
                return Err(MinervaScraperError::ParseError(format!(
                    "Encountered unexpected row with {} children",
                    children.len()
                ))
                .into());
            }
        }
    }

    log::debug!("Parsed {} rows", data_rows.len());
    Ok(data_rows)
}
