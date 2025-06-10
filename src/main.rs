#![feature(string_deref_patterns)]
#![feature(if_let_guard)]
#![feature(option_zip)]

use std::ops::Add;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

use anyhow::Context;

use chrono::{TimeDelta, Utc};

use rusqlite::Connection;

use tokio::sync::mpsc::UnboundedSender;

mod db;
mod discord;
// mod models;
mod parser;
mod scraper;

use crate::db::{
    SectionRecord, bump_timestamp, demote_latest, insert_section, insert_terms_from_map,
    last_update, latest_section_from_term_crn, open_or_init, subscriptions_to_section, term_ids,
    term_name_from_id,
};
use crate::discord::discord_thread_entry;
use crate::scraper::ScraperState;

pub struct SectionChangeMessage {
    channels: Vec<(u64, bool)>,
    prev: Option<SectionRecord>,
    next: SectionRecord,
    term_name: String,
}

fn insert_sections_for_term(
    sc: &mut ScraperState,
    req_delay: Duration,
    term: u32,
    conn: &mut Connection,
    sender: &UnboundedSender<SectionChangeMessage>,
) -> Result<(), anyhow::Error> {
    log::debug!("Fetching sections for term {term}");

    let (subj_ids, fac_ids) = sc.get_subj_fac_codes_for_term(term)?;
    let term_name = term_name_from_id(conn, term)?
        .ok_or(anyhow::anyhow!(format!("No terms entry found for {term}")))?;

    for fac in fac_ids {
        sleep(req_delay);
        let now = Utc::now();
        let sections =
            sc.get_sections_for_term_subjs_fac(term, subj_ids.as_slice(), fac.as_str(), true)?;

        let trx = conn.transaction()?;
        let mut ctr = 0;
        for sec in sections {
            let prev = latest_section_from_term_crn(&trx, term, sec.crn)?;
            let next = SectionRecord {
                term,
                timestamp: now,
                inner: sec,
            };

            if next.is_equivalent(prev.as_ref()) {
                bump_timestamp(&trx, term, next.inner.crn, now)?;
            } else {
                demote_latest(&trx, term, next.inner.crn)?;
                insert_section(&trx, &next, true).with_context(|| {
                    format!(
                        "Could not insert {} {}-{} ({}: {})",
                        next.inner.subject,
                        next.inner.number,
                        next.inner.section,
                        term,
                        next.inner.crn
                    )
                })?;
                ctr += 1;

                let channels = subscriptions_to_section(&trx, term, next.inner.crn)?;
                // Just do a simple subscriber check here, and do the rest of the
                // filtering on the listener thread, to avoid duplicating too much logic
                if !channels.is_empty() {
                    log::debug!(
                        "Sending change for {term}-{} for {channels:?}",
                        next.inner.crn
                    );
                    sender.send(SectionChangeMessage {
                        channels,
                        prev,
                        next,
                        term_name: term_name.clone(),
                    })?
                } else {
                    log::trace!(
                        "Ignoring change for {term}-{}, no subscriptions",
                        next.inner.crn
                    );
                }
            }
        }
        trx.commit()?;
        log::trace!("Committed {ctr} new section records");
    }
    Ok(())
}

fn insert_all_sections(
    sc: &mut ScraperState,
    req_delay: Duration,
    conn: &mut Connection,
    sender: &UnboundedSender<SectionChangeMessage>,
    minimum_term: u32,
) -> Result<(), anyhow::Error> {
    log::info!("Fetching all sections");

    let terms = term_ids(conn, minimum_term)?;

    for term in terms {
        insert_sections_for_term(sc, req_delay, term, conn, sender)?;
    }
    Ok(())
}

fn insert_terms(
    sc: &mut ScraperState,
    conn: &mut Connection,
    include_ro: bool,
) -> Result<(), anyhow::Error> {
    log::info!("Fetching available terms");
    let map = sc.get_term_map(include_ro)?;

    insert_terms_from_map(conn, map)?;
    Ok(())
}

fn open_db() -> Result<(Connection, PathBuf), anyhow::Error> {
    let db_path = {
        let default_file = format!("{}.sqlite", env!("CARGO_BIN_NAME"));
        if let Ok(path) = std::env::var("CW_DB_PATH") {
            let rv = PathBuf::from(path);
            if rv.is_dir() {
                rv.join(default_file)
            } else {
                rv
            }
        } else {
            log::info!("missing or non-unicode CW_DB_PATH, falling back to STATE_DIRECTORY");
            let dir =
                std::env::var("STATE_DIRECTORY").expect("missing or non-unicode STATE_DIRECTORY");
            PathBuf::from(dir).join(default_file)
        }
    };
    let rv = open_or_init(&db_path, true)?;
    log::info!("Opened database at {}", db_path.to_string_lossy());
    Ok((rv, db_path))
}

fn init_logger() {
    let mut logger_builder = env_logger::builder();
    logger_builder
        .format_timestamp_millis()
        .filter_level(log::LevelFilter::Warn)
        .filter_module("coursewatcher", log::LevelFilter::Debug);

    if std::env::var_os("SYSTEMD_EXEC_PID").is_some() {
        logger_builder.format_timestamp(None);
    }

    logger_builder.init();
}

fn main() {
    init_logger();

    log::info!(
        "Starting {} {}",
        env!("CARGO_BIN_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    // Get all envvars
    let uid = std::env::var("CW_MINERVA_UID").expect("missing or non-unicode CW_MINERVA_UID");
    let pin = std::env::var("CW_MINERVA_PIN").expect("missing or non-unicode CW_MINERVA_PIN");

    let discord_token =
        std::env::var("DISCORD_TOKEN").expect("missing or non-unicode DISCORD_TOKEN");

    // Wipe sensitive envvars
    // SAFETY: no other threads launched yet at this point
    unsafe {
        std::env::remove_var("CW_MINERVA_UID");
        std::env::remove_var("CW_MINERVA_PIN");
        std::env::remove_var("DISCORD_TOKEN");
    }

    let cycle_min = std::env::var("CW_CYCLE_DELAY_MIN")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(5);
    let request_delay_sec = std::env::var("CW_REQUEST_DELAY_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(3);

    let minimum_term = std::env::var("CW_MINIMUM_TERM")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);

    let (mut conn, db_path) = open_db().unwrap();

    log::info!("Configuration and database loaded");
    log::info!("Using {cycle_min} min cycle delay and {request_delay_sec} sec request delay",);

    let mut scraper = ScraperState::new(None).unwrap();
    scraper.login(&uid, &pin, None).unwrap();

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(|| discord_thread_entry(discord_token, db_path, receiver));

    if std::env::var_os("CW_IMMEDIATE_FETCH").is_none() {
        let last_update = last_update(&conn).unwrap().unwrap_or_default();
        let neg_elapsed = last_update.signed_duration_since(Utc::now());
        let remaining = neg_elapsed
            .add(TimeDelta::minutes(cycle_min as i64))
            .max(TimeDelta::zero());
        sleep(remaining.to_std().unwrap())
    }

    insert_terms(&mut scraper, &mut conn, false).unwrap();

    loop {
        if scraper.check_login().is_err() {
            log::warn!("Session no longer valid, attempting login");
            scraper.login(&uid, &pin, None).unwrap();
        }

        insert_all_sections(
            &mut scraper,
            Duration::from_secs(request_delay_sec),
            &mut conn,
            &sender,
            minimum_term,
        )
        .unwrap();

        log::trace!("Finished fetching, sleeping");

        sleep(Duration::from_secs(cycle_min * 60));
    }
}
