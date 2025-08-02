#![feature(string_deref_patterns)]
#![feature(if_let_guard)]
#![feature(option_zip)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

use anyhow::Context;

use chrono::Utc;

use reqwest::blocking::Client;

use rusqlite::{Connection, Transaction};

use tokio::sync::mpsc::UnboundedSender;

mod db;
mod discord;
// mod models;
mod minerva;
mod vsb;

use crate::db::{
    SectionRecord, bump_timestamp, demote_latest_by_crn, demote_latest_by_number, insert_section,
    insert_terms_from_map, latest_section_from_term_crn, open_or_init, subscriptions_term_crn_all,
    subscriptions_to_section, term_ids,
};
use crate::discord::discord_thread_entry;
use crate::minerva::{
    check_login, get_sections_for_term_subjs_fac, get_subj_fac_codes_for_term, get_term_map, login,
    new_client,
};
use crate::vsb::vsb_get;

pub struct SectionChangeMessage {
    channels: Vec<(u64, bool)>,
    prev: Option<SectionRecord>,
    next: SectionRecord,
    term_name: String,
}

fn process_section_update(
    prev: Option<SectionRecord>,
    next: SectionRecord,
    trx: &Transaction,
    sender: &UnboundedSender<SectionChangeMessage>,
    term_map: &HashMap<u32, String>,
) -> Result<bool, anyhow::Error> {
    if next.is_equivalent(prev.as_ref()) {
        bump_timestamp(trx, next.term, next.inner.crn, next.timestamp)?;
        Ok(false)
    } else {
        demote_latest_by_crn(trx, next.term, next.inner.crn)?;
        // This is necessary because sometimes a section's section number is reassigned
        // And so we need to bump the previous record holding that section number out of the way
        demote_latest_by_number(
            trx,
            next.term,
            next.inner.subject,
            next.inner.number.clone(),
            next.inner.section.clone(),
        )?;
        insert_section(trx, &next, true).with_context(|| {
            format!(
                "Could not insert {} {}-{} ({}: {})",
                next.inner.subject,
                next.inner.number,
                next.inner.section,
                next.term,
                next.inner.crn
            )
        })?;

        let channels = subscriptions_to_section(trx, next.term, next.inner.crn)?;
        // We do the real filtering on the listener thread, but because minerva fetches
        // every course, do a quick subscriber check here to avoid sending so many
        // unnecessary updates
        if !channels.is_empty() {
            log::debug!(
                "Sending change for {}-{} for {channels:?}",
                next.term,
                next.inner.crn
            );
            sender.send(SectionChangeMessage {
                term_name: term_map
                    .get(&next.term)
                    .cloned()
                    .unwrap_or("Unknown Term".to_string()),
                channels,
                prev,
                next,
            })?
        } else {
            log::trace!(
                "Ignoring change for {}-{}, no subscriptions",
                next.term,
                next.inner.crn
            );
        }
        Ok(true)
    }
}

fn insert_sections_for_term(
    cl: &Client,
    term: u32,
    conn: &mut Connection,
    sender: &UnboundedSender<SectionChangeMessage>,
    term_map: &HashMap<u32, String>,
) -> Result<(), anyhow::Error> {
    log::debug!("Fetching sections for term {term}");

    let (subj_ids, _) = get_subj_fac_codes_for_term(cl, term)?;

    let now = Utc::now();
    let sections = get_sections_for_term_subjs_fac(cl, term, subj_ids.as_slice(), None, true)?;

    let trx = conn.transaction()?;
    let mut ctr = 0;
    for sec in sections {
        let prev = latest_section_from_term_crn(&trx, term, sec.crn)?;
        let next = SectionRecord {
            term,
            timestamp: now,
            inner: sec,
        };

        ctr += process_section_update(prev, next, &trx, sender, term_map)? as u32;
    }
    trx.commit()?;
    log::trace!("Committed {ctr} new section records from Minerva");
    Ok(())
}

fn insert_all_sections(
    cl: &Client,
    conn: &mut Connection,
    sender: &UnboundedSender<SectionChangeMessage>,
    term_map: &HashMap<u32, String>,
    minimum_term: u32,
) -> Result<(), anyhow::Error> {
    log::info!("Fetching all sections");

    let terms = term_ids(conn, minimum_term)?;

    // TODO maybe parallelize?

    for term in terms {
        insert_sections_for_term(cl, term, conn, sender, term_map)?;
    }
    Ok(())
}

fn insert_terms(
    cl: &Client,
    conn: &mut Connection,
    include_ro: bool,
) -> Result<HashMap<u32, String>, anyhow::Error> {
    log::info!("Fetching available terms");
    let map = get_term_map(cl, include_ro)?;

    insert_terms_from_map(conn, &map)?;
    Ok(map)
}

fn update_subbed_vsb(
    cl: &Client,
    conn: &mut Connection,
    sender: &UnboundedSender<SectionChangeMessage>,
    term_map: &HashMap<u32, String>,
) -> Result<(), anyhow::Error> {
    log::trace!("Fetching subscribed occupancy from VSB");

    let terms_crns = subscriptions_term_crn_all(conn)?;

    let results = vsb_get(cl, &terms_crns)?;
    let trx = conn.transaction()?;
    let mut ctr = 0;
    for data in results {
        let prev = match latest_section_from_term_crn(&trx, data.term, data.crn)? {
            Some(d) => d,
            None => continue,
        };
        let next = data.merge_with_full(prev.clone())?;

        ctr += process_section_update(Some(prev), next, &trx, sender, term_map)? as u32;
    }
    trx.commit()?;
    log::trace!("Committed {ctr} new section records from VSB");
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

    // Parse env last so that it overrides
    logger_builder.parse_default_env().init();
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

    let cycle_sec = std::env::var("CW_CYCLE_DELAY_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(10);
    let full_fetch_cycles = std::env::var("CW_FULL_FETCH_EVERY")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(360);
    let minimum_term = std::env::var("CW_MINIMUM_TERM")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);

    let (mut conn, db_path) = open_db().unwrap();

    log::info!("Configuration and database loaded");
    log::info!("Using {cycle_sec} sec cycle delay");
    log::info!("Full minerva fetches every {full_fetch_cycles} cycles");

    let cl = new_client().unwrap();
    login(&cl, &uid, &pin).unwrap();

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(|| discord_thread_entry(discord_token, db_path, receiver));

    let term_map = insert_terms(&cl, &mut conn, false).unwrap();

    let mut cycle_ctr = 0;
    loop {
        if cycle_ctr == 0 {
            if check_login(&cl).is_err() {
                log::warn!("Session no longer valid, attempting login");
                login(&cl, &uid, &pin).unwrap();
            }

            insert_all_sections(&cl, &mut conn, &sender, &term_map, minimum_term).unwrap();
        } else {
            update_subbed_vsb(&cl, &mut conn, &sender, &term_map).unwrap();
        }

        if full_fetch_cycles != 0 {
            cycle_ctr = (cycle_ctr + 1) % full_fetch_cycles
        }

        log::trace!("Finished fetching, sleeping");

        sleep(Duration::from_secs(cycle_sec));
    }
}
