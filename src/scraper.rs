use std::collections::HashMap;
use std::fmt::Debug;
use std::num::ParseIntError;
use std::sync::Arc;

use thiserror::Error;

use anyhow::Context;

use reqwest::Url;
use reqwest::blocking::{Client, ClientBuilder, Request, Response};
use reqwest::cookie::Jar;
use reqwest::header::HeaderValue;

use scraper::{Html, Selector};

use crate::parser::{CourseNumber, SectionData, SectionNumber};

#[derive(Debug)]
pub struct ScraperState {
    base_url: Url,
    cookies: Arc<Jar>,
    client: Client,
    prev_url: String,
}

#[derive(Debug, Error)]
pub enum ScraperError {
    #[error("Failed to log in: Authorization Failure")]
    LoginFailure,
    #[error("Not currently logged in")]
    LoggedOut,
    #[error("No such section was found in the given course")]
    NoSection,
    #[error("Failed to parse the results table: {0}")]
    ParseError(String),
}

impl ScraperState {
    pub fn new(base: Option<String>) -> Result<Self, anyhow::Error> {
        let base_string = base
            .clone()
            .unwrap_or_else(|| "https://horizon.mcgill.ca/".to_string());

        let base_url = Url::parse(&base_string)?;
        let jar = Arc::new(Jar::default());
        let client = ClientBuilder::new()
            .cookie_provider(jar.clone())
            .https_only(true)
            .user_agent(format!(
                "{} / {}",
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            ))
            .build()?;

        Ok(Self {
            base_url,
            prev_url: base_string.clone(),
            cookies: jar,
            client,
        })
    }

    fn abs_url(&self, rel_path: &str) -> Result<Url, anyhow::Error> {
        Ok(self.base_url.join("pban1/")?.join(rel_path)?)
    }

    fn send_req(&mut self, mut req: Request) -> Result<Response, anyhow::Error> {
        if !req.headers().contains_key("referer") {
            req.headers_mut()
                .insert("referer", HeaderValue::from_str(&self.prev_url)?);
            self.prev_url = req.url().to_string();
        }

        self.client
            .execute(req)?
            .error_for_status()
            .map_err(|e| e.into())
    }

    fn quick_get(&mut self, rel_path: &str) -> Result<Response, anyhow::Error> {
        let url = self.abs_url(rel_path)?;

        let req = self.client.get(url).build()?;

        self.send_req(req)
    }

    fn quick_post(&mut self, rel_path: &str, data: String) -> Result<Response, anyhow::Error> {
        let url = self.abs_url(rel_path)?;
        let req = self
            .client
            .post(url)
            .body(data)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .build()?;

        self.send_req(req)
    }

    pub fn check_login(&mut self) -> Result<(), anyhow::Error> {
        if self
            .quick_get("twbkwbis.P_GenMenu?name=bmenu.P_MainMnu")?
            .text()?
            .contains("/pban1/twbkwbis.P_ValLogin")
        {
            log::info!("Session is no longer valid");
            Err(ScraperError::LoggedOut.into())
        } else {
            log::debug!("Session is still valid");
            Ok(())
        }
    }

    pub fn login(
        &mut self,
        uid: &str,
        pin: &str,
        sessid: Option<&str>,
    ) -> Result<String, anyhow::Error> {
        if let Some(sessid) = sessid {
            log::trace!("Validating given SESSID");

            // .expect() is fine since the base url is constant
            let cookie = format!(
                "SESSID={}; Domain=.{}; HTTPOnly; SameSite=None; Secure",
                sessid,
                self.base_url.host_str().expect("base URL is missing host")
            );
            self.cookies.add_cookie_str(&cookie, &self.base_url);

            if self.check_login().is_ok() {
                log::debug!("Sucessfully authenticated with given SESSID");
                return Ok(sessid.to_string());
            } else {
                log::warn!("Falling back to UID/PIN login");
            }
        }

        log::trace!("Attempting UID/PIN login");

        self.quick_get("twbkwbis.P_WWWLogin")?;

        let resp = self.quick_post("twbkwbis.P_ValLogin", format!("sid={uid}&PIN={pin}"))?;

        if let Some(sessid) = resp.cookies().find(|c| c.name() == "SESSID")
            && !sessid.value().is_empty()
        {
            self.quick_get("twbkwbis.P_GenMenu?name=bmenu.P_MainMnu")?;

            log::debug!("Authenticated using UID/PIN");

            Ok(sessid.value().to_string())
        } else {
            Err(ScraperError::LoginFailure.into())
        }
    }

    pub fn get_term_map(
        &mut self,
        include_ro: bool,
    ) -> Result<HashMap<String, u32>, anyhow::Error> {
        log::trace!(
            "Fetching {} terms",
            if include_ro {
                "all"
            } else {
                "currently active"
            }
        );

        let resp = self.quick_get("bwskfcls.p_sel_crse_search")?;
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
            .map(|e| (e.inner_html(), e.attr("value").unwrap_or("").to_string()))
            .filter(|(_k, v)| !v.is_empty())
            .filter(|(k, _v)| include_ro || !k.ends_with("(View only)"))
            .map(|(k, vs)| {
                vs.parse()
                    .map(|v| (k, v))
                    .map_err(|e: ParseIntError| e.into())
            })
            .collect::<Result<HashMap<String, u32>, anyhow::Error>>();

        log::debug!(
            "Found {} terms",
            terms.as_ref().map(|v| v.len()).unwrap_or(0)
        );

        terms
    }

    pub fn get_subj_fac_codes_for_term(
        &mut self,
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

        let resp = self.quick_post("bwskfcls.P_GetCrse", form_data)?;
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
        Ok((subj_ids, fac_ids))
    }

    pub fn get_sections_for_term_subjs_fac(
        &mut self,
        term: u32,
        subj_ids: &[String],
        fac_id: &str,
        skip_error_rows: bool,
    ) -> Result<Vec<SectionData>, anyhow::Error> {
        log::trace!(
            "Fetching sections for {} subjects in {} during {}",
            subj_ids.len(),
            fac_id,
            term
        );

        let form_data = format!(
            "rsts=dummy&crn=dummy&sel_subj=dummy&\
                sel_subj={}&term_in={term}&sel_coll={fac_id}&\
                sel_day=dummy&sel_schd=dummy&sel_insm=dummy&\
                sel_camp=dummy&sel_levl=dummy&sel_sess=dummy&sel_instr=dummy&sel_ptrm=dummy&\
                sel_attr=dummy&\
                sel_crse=&sel_title=&sel_schd=%25&sel_from_cred=&sel_to_cred=&sel_levl=%25&\
                sel_ptrm=%25&sel_instr=%25&sel_attr=%25&begin_hh=0&begin_mi=0&begin_ap=a&end_hh=0&\
                end_mi=0&end_ap=a&SUB_BTN=Get+Course+Sections&path=1",
            subj_ids.join("&sel_subj=")
        );

        let resp = self.quick_post("bwskfcls.P_GetCrse_Advanced", form_data)?;
        let text = resp.text()?;

        self.parse_results_table(&text, skip_error_rows)
            .with_context(|| {
                format!(
                    "Error parsing courses in {fac_id} during {term} for {} subjects",
                    subj_ids.len()
                )
            })
    }

    pub fn get_section_for_term_subj_number_section(
        &mut self,
        term: u32,
        subject: &str,
        number: &CourseNumber,
        section: &SectionNumber,
        skip_error_rows: bool,
    ) -> Result<SectionData, anyhow::Error> {
        self.get_sections_for_term_subj_number(term, subject, number, skip_error_rows)?
            .into_iter()
            .find(|sec| &sec.section == section)
            .ok_or_else(|| ScraperError::NoSection.into())
    }

    pub fn get_sections_for_term_subj_number(
        &mut self,
        term: u32,
        subject: &str,
        number: &CourseNumber,
        skip_error_rows: bool,
    ) -> Result<Vec<SectionData>, anyhow::Error> {
        log::trace!("Fetching sections for course {subject} {number} during {term}");

        let form_data = format!(
            "term_in={term}&\
            sel_subj=dummy&\
            sel_subj={subject}&\
            SEL_CRSE={number}&\
            SEL_TITLE=&BEGIN_HH=0&BEGIN_MI=0&BEGIN_AP=a&SEL_DAY=dummy&SEL_PTRM=dummy&END_HH=0&\
            END_MI=0&END_AP=a&SEL_CAMP=dummy&SEL_SCHD=dummy&SEL_SESS=dummy&SEL_INSTR=dummy&\
            SEL_INSTR=%25&SEL_ATTR=dummy&SEL_ATTR=%25&SEL_LEVL=dummy&SEL_LEVL=%25&SEL_INSM=dummy&\
            sel_dunt_code=&sel_dunt_unit=&call_value_in=&rsts=dummy&path=1&SUB_BTN=View+Sections",
        );

        let resp = self.quick_post("bwskfcls.P_GetCrse", form_data)?;
        let text = resp.text()?;

        let sections = self.parse_results_table(&text, skip_error_rows)?;

        Ok(sections)
    }

    pub fn parse_results_table(
        &self,
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
            .ok_or_else(|| ScraperError::ParseError("Missing title row".to_string()))?;
        let key_row = table_rows
            .next()
            .ok_or_else(|| ScraperError::ParseError("Missing key row".to_string()))?;

        // to ensure the format hasn't changed unexpectedly
        let col_count = key_row.child_elements().count();
        if col_count != 20 {
            return Err(ScraperError::ParseError(format!(
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
                    log::trace!("Skipping supplementary time row (TODO)")
                } else {
                    if skip_error_rows {
                        log::warn!("Ignoring unknown 20-cell row format: {ch_array:?}");
                    } else {
                        return Err(ScraperError::ParseError(format!(
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
                            return Err(ScraperError::ParseError(format!(
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
                    return Err(ScraperError::ParseError(format!(
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
}
