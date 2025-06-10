use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;

use rusqlite::Connection;

use serenity::all::{
    Command, CommandDataOption, CommandDataOptionValue, CommandInteraction, CommandOptionType,
    CreateEmbedFooter, EditInteractionResponse, ResolvedOption, ResolvedValue,
};
use serenity::builder::{
    CreateCommand, CreateCommandOption, CreateEmbed, CreateEmbedAuthor, CreateInteractionResponse,
    CreateInteractionResponseMessage,
};
use serenity::model::Colour;
use serenity::model::application::Interaction;
use serenity::model::gateway::Ready;
use serenity::prelude::{Client, Context, EventHandler, GatewayIntents};

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Mutex, RwLock};
use tokio::task::{JoinHandle, block_in_place};

mod listener;

use crate::SectionChangeMessage;
use crate::db::{
    db_stats, lookup_latest_sections_with_term_name, open_or_init, parse_term,
    subscribe_channel_to_section_by_crn, subscribe_channel_to_section_by_subj_number,
    subscriptions_with_latest_description_for_channel, unsubscribe_channel_from_section_by_crn,
    unsubscribe_channel_to_section_by_subj_number,
};
use crate::discord::listener::create_section_update_embed;
use crate::parser::{CourseNumber, SectionNumber, Subject};

const MARTLET_PNG_URL: &str = "https://cdn.discordapp.com/attachments/1385507660668993648/1390416756413304902/VeeM0cK.png?ex=68682e03&is=6866dc83&hm=4bb37f9ce82553d8613780b29e3e7f972e43d6f15728cd0d9e9457be7ec77183&";

struct Handler {
    db_conn: Mutex<Connection>,
    receiver: Mutex<Option<UnboundedReceiver<SectionChangeMessage>>>,
    receiver_started: AtomicBool,
    subscription_list_cache: RwLock<HashMap<u64, Vec<String>>>,
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: Ready) {
        log::info!("Connected to gateway as {}", ready.user.name);

        let course_options = [
            CreateCommandOption::new(
                CommandOptionType::String,
                "term",
                "The term name or code to search in, like \"Fall 2025\" or \"202509\".",
            )
            .required(true),
            CreateCommandOption::new(
                CommandOptionType::Integer,
                "crn",
                "The Course Registration Number to search for, like \"2131\".",
            )
            .required(true)
            .min_int_value(1),
            CreateCommandOption::new(
                CommandOptionType::String,
                "subject",
                "The subject code to search in, like \"BIOL\".",
            )
            .required(true)
            .min_length(4)
            .max_length(4),
            CreateCommandOption::new(
                CommandOptionType::String,
                "number",
                "The course number to search for, like \"133\" or \"349D2\".",
            )
            .required(true),
            CreateCommandOption::new(
                CommandOptionType::String,
                "section",
                "The specific section to search for, like \"1\" or \"002\".\
                    All sections are included if omitted.",
            )
            .required(false),
            CreateCommandOption::new(
                CommandOptionType::Boolean,
                "allspans",
                "Implicitly include spanned courses, like \"PSYC 349D\" \
                                        if just \"PSYC 349\" is searched. Default is no.",
            )
            .required(false),
            CreateCommandOption::new(
                CommandOptionType::Boolean,
                "allupdates",
                "Send updates even if the section is full.",
            )
            .required(false),
        ];

        Command::set_global_commands(
            &ctx.http,
            vec![
                CreateCommand::new("status").description("Show miscellaneous bot information"),
                CreateCommand::new("sub")
                    .description("Watch a course in this channel")
                    .add_option(course_options[0].clone())
                    .add_option(course_options[2].clone())
                    .add_option(course_options[3].clone())
                    .add_option(course_options[4].clone())
                    .add_option(course_options[6].clone()),
                CreateCommand::new("unsub")
                    .description("Unwatch a course in this channel")
                    .add_option(course_options[0].clone())
                    .add_option(course_options[2].clone())
                    .add_option(course_options[3].clone())
                    .add_option(course_options[4].clone()),
                CreateCommand::new("subcrn")
                    .description("Watch a course in this channel by CRN")
                    .add_option(course_options[0].clone())
                    .add_option(course_options[1].clone())
                    .add_option(course_options[6].clone()),
                CreateCommand::new("unsubcrn")
                    .description("Unwatch a course in this channel by CRN")
                    .add_option(course_options[0].clone())
                    .add_option(course_options[1].clone()),
                CreateCommand::new("list")
                    .description("List watched courses in this channel")
                    .add_option(
                        CreateCommandOption::new(
                            CommandOptionType::Integer,
                            "page",
                            "Which 24-section page of the list to fetch",
                        )
                        .required(false)
                        .min_int_value(1),
                    ),
                CreateCommand::new("search")
                    .description("Show details about the first 10 matching sections")
                    .add_option(course_options[0].clone().required(false))
                    .add_option(course_options[1].clone().required(false))
                    .add_option(course_options[2].clone().required(false))
                    .add_option(course_options[3].clone().required(false))
                    .add_option(course_options[4].clone())
                    .add_option(course_options[5].clone()),
            ],
        )
        .await
        .unwrap();

        // Use the strongest ordering just to be safe
        if self
            .receiver_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // Unwrap OK because this section can only run once ever, so value is always `Some`
            let receiver = self.receiver.lock().await.take().unwrap();
            tokio::spawn(async move { listener::section_change_listener(ctx, receiver).await });
        }
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        if let Interaction::Command(command) = interaction {
            match command.data.name.as_str() {
                "status" => self.status(ctx, command).await,
                "list" => self.list_subscriptions(ctx, command).await,
                "search" => self.lookup_course_wrapper(ctx, command).await,
                "subcrn" => {
                    let res = self.add_subscription_by_crn(&command).await;
                    self.response_wrapper(ctx, command, res).await;
                }
                "unsubcrn" => {
                    let res = self.remove_subscription_by_crn(&command).await;
                    self.response_wrapper(ctx, command, res).await;
                }
                "sub" => {
                    let res = self.add_subscription(&command).await;
                    self.response_wrapper(ctx, command, res).await;
                }
                "unsub" => {
                    let res = self.remove_subscription(&command).await;
                    self.response_wrapper(ctx, command, res).await;
                }
                _ => {
                    let msg = CreateInteractionResponseMessage::new()
                        .content("_Not yet implemented._")
                        .ephemeral(true);
                    command
                        .create_response(&ctx, CreateInteractionResponse::Message(msg))
                        .await
                        .unwrap();
                }
            };
        }
    }
}

impl Handler {
    fn check_manage_perm(interaction: &CommandInteraction) -> bool {
        if let Some(member) = &interaction.member {
            // Unwrap OK because this is from an interaction
            let perms = member.permissions.unwrap();
            perms.manage_channels() || perms.manage_webhooks() || perms.administrator()
        } else {
            // In DMs, always OK
            true
        }
    }

    async fn response_wrapper(
        &self,
        ctx: Context,
        interaction: CommandInteraction,
        result: Result<(bool, impl Into<String>), anyhow::Error>,
    ) {
        match result {
            Ok((e, s)) => {
                let msg = CreateInteractionResponseMessage::new()
                    .content(s)
                    .ephemeral(e);
                interaction
                    .create_response(ctx, CreateInteractionResponse::Message(msg))
                    .await
                    .unwrap();
            }
            Err(e) => {
                let msg = CreateInteractionResponseMessage::new()
                    .content("_An internal service error has occurred_")
                    .ephemeral(true);
                interaction
                    .create_response(ctx, CreateInteractionResponse::Message(msg))
                    .await
                    .unwrap();
                log::error!("Unhandled error while processing command: {e:?}")
            }
        };
    }

    fn defer_bg(
        ctx: &Context,
        interaction: &CommandInteraction,
        ephemeral: bool,
    ) -> JoinHandle<()> {
        let ctx = ctx.clone();
        let interaction = interaction.clone();
        if ephemeral {
            let fut = async move { interaction.defer_ephemeral(ctx).await.unwrap() };
            tokio::spawn(fut)
        } else {
            let fut = async move { interaction.defer(ctx).await.unwrap() };
            tokio::spawn(fut)
        }
    }

    async fn get_term_crn_allupdates_args(
        &self,
        options: Vec<ResolvedOption<'_>>,
    ) -> Result<(u32, u32, bool), &'static str> {
        let mut term = None;
        let mut crn = None;
        let mut all_updates = false;
        for option in options {
            match (option.name, option.value) {
                ("term", ResolvedValue::String(val)) => {
                    let conn = self.db_conn.lock().await;
                    let res = block_in_place(|| parse_term(conn.deref(), val));
                    match res {
                        Ok(Some(t)) => term = Some(t),
                        Ok(None) => return Err("Unknown term number or name"),
                        Err(e) => {
                            log::error!("Error looking up term for {val}: {e}");
                            return Err("_Internal error looking up term_");
                        }
                    }
                }
                ("crn", ResolvedValue::Integer(val)) => crn = Some(val as u32),
                ("allupdates", ResolvedValue::Boolean(val)) => all_updates = val,
                (o, v) => {
                    log::error!("Unknown argument {o} passed to subcrn or unsubcrn command: {v:?}");
                    return Err("_Internal error while parsing command arguments_");
                }
            }
        }

        if term.is_none() || crn.is_none() {
            return Err("_Internal error while parsing command arguments_");
        }

        Ok((term.unwrap(), crn.unwrap(), all_updates))
    }

    async fn add_subscription_by_crn(
        &self,
        interaction: &CommandInteraction,
    ) -> Result<(bool, &'static str), anyhow::Error> {
        if !Self::check_manage_perm(interaction) {
            return Ok((
                true,
                "You need the \"Manage Channels\" or \"Manage Webhooks\" permission in this channel to add subscriptions",
            ));
        }

        let (term, crn, all_updates) = match self
            .get_term_crn_allupdates_args(interaction.data.options())
            .await
        {
            Ok(tc) => tc,
            Err(s) => return Ok((true, s)),
        };

        let insertion: Result<(), rusqlite::Error> = {
            let mut conn = self.db_conn.lock().await;
            block_in_place(|| {
                let trx = conn.transaction()?;
                subscribe_channel_to_section_by_crn(
                    &trx,
                    interaction.channel_id.get(),
                    term,
                    crn,
                    all_updates,
                )?;
                trx.commit()
            })
        };

        match insertion {
            Ok(()) => Ok((false, "Added/updated 1 course subscription")),
            Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
                },
                ..,
            ))
            | Err(rusqlite::Error::StatementChangedRows(0)) => Ok((
                true,
                "This channel is already subscribed to updates for the requested course",
            )),
            Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER,
                },
                ..,
            )) => Ok((true, "Could not find section with given term and CRN")),
            Err(e) => Err(e.into()),
        }
    }

    async fn remove_subscription_by_crn(
        &self,
        interaction: &CommandInteraction,
    ) -> Result<(bool, &'static str), anyhow::Error> {
        if !Self::check_manage_perm(interaction) {
            return Ok((
                true,
                "You need the \"Manage Channels\" or \"Manage Webhooks\" permission in this channel to remove subscriptions",
            ));
        }

        let (term, crn, _) = match self
            .get_term_crn_allupdates_args(interaction.data.options())
            .await
        {
            Ok(tc) => tc,
            Err(s) => return Ok((true, s)),
        };

        let removal = {
            let conn = self.db_conn.lock().await;
            block_in_place(|| {
                unsubscribe_channel_from_section_by_crn(
                    &conn,
                    interaction.channel_id.get(),
                    term,
                    crn,
                )
            })
        };

        match removal {
            Ok(()) => {
                // drop cache for this channel
                self.subscription_list_cache
                    .write()
                    .await
                    .remove(&interaction.channel_id.get());
                Ok((false, "Removed subscription for 1 new course"))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Ok((true, "This channel is not subscribed to the given course"))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn get_term_subj_course_sec_args(
        &self,
        options: Vec<ResolvedOption<'_>>,
    ) -> Result<
        (
            u32,
            Subject,
            CourseNumber,
            Option<SectionNumber>,
            bool,
            bool,
        ),
        &'static str,
    > {
        let mut term = None;
        let mut subj = None;
        let mut course = None;
        let mut section = None;
        let mut all_spans = false;
        let mut all_updates = false;
        for option in options {
            match (option.name, option.value) {
                ("term", ResolvedValue::String(val)) => {
                    let conn = self.db_conn.lock().await;
                    let res = block_in_place(|| parse_term(conn.deref(), val));
                    match res {
                        Ok(Some(t)) => term = Some(t),
                        Ok(None) => return Err("Unknown term number or name"),
                        Err(e) => {
                            log::error!("Error looking up term for {val}: {e}");
                            return Err("_Internal error looking up term_");
                        }
                    }
                }
                ("subject", ResolvedValue::String(val)) => {
                    subj = Some(
                        Subject::try_from(val)
                            .map_err(|_| "Subject code was not 4 letters long")?,
                    )
                }
                ("number", ResolvedValue::String(val)) => {
                    course = Some(
                        CourseNumber::try_from(val.to_string())
                            .map_err(|_| "Course number cannot be empty")?,
                    )
                }
                ("section", ResolvedValue::String(val)) => {
                    section = SectionNumber::try_from(val.to_string()).ok()
                }
                ("allspans", ResolvedValue::Boolean(val)) => all_spans = val,
                ("allupdates", ResolvedValue::Boolean(val)) => all_updates = val,
                (o, v) => {
                    log::error!("Unknown argument {o} passed to sub or unsub command: {v:?}");
                    return Err("_Internal error while parsing command arguments_");
                }
            }
        }

        if term.is_none() || subj.is_none() || course.is_none() {
            return Err("_Internal error while parsing command arguments_");
        }

        Ok((
            term.unwrap(),
            subj.unwrap(),
            course.unwrap(),
            section,
            all_spans,
            all_updates,
        ))
    }

    async fn add_subscription(
        &self,
        interaction: &CommandInteraction,
    ) -> Result<(bool, String), anyhow::Error> {
        if !Self::check_manage_perm(interaction) {
            return Ok((
                true,
                "You need the \"Manage Channels\" or \"Manage Webhooks\" permission in this channel to add subscriptions".to_string(),
            ));
        }

        let (term, subj, course, sec, all_spans, all_updates) = match self
            .get_term_subj_course_sec_args(interaction.data.options())
            .await
        {
            Ok(vals) => vals,
            Err(s) => return Ok((true, s.to_string())),
        };

        let insertion = {
            let conn = self.db_conn.lock().await;
            block_in_place(|| {
                subscribe_channel_to_section_by_subj_number(
                    &conn,
                    interaction.channel_id.get(),
                    term,
                    subj,
                    course,
                    sec,
                    all_spans,
                    all_updates,
                )
            })
        };

        match insertion {
            Ok(0) => Ok((
                true,
                "Could not find section matching given query".to_string(),
            )),
            Ok(1) => Ok((false, "Added/updated 1 section subscription".to_string())),
            Ok(n) => Ok((false, format!("Added/updated {n} section subscriptions"))),
            Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
                },
                ..,
            )) => Ok((
                true,
                "This channel is already subscribed to updates for the requested course"
                    .to_string(),
            )),
            Err(e) => {
                log::error!("Unexpected error attempting to insert new subscription: {e:?}");
                Err(e.into())
            }
        }
    }

    async fn remove_subscription(
        &self,
        interaction: &CommandInteraction,
    ) -> Result<(bool, String), anyhow::Error> {
        if !Self::check_manage_perm(interaction) {
            return Ok((
                true,
                "You need the \"Manage Channels\" or \"Manage Webhooks\" permission in this channel to remove subscriptions".to_string(),
            ));
        }

        let (term, subj, course, sec, all_spans, _) = match self
            .get_term_subj_course_sec_args(interaction.data.options())
            .await
        {
            Ok(vals) => vals,
            Err(s) => return Ok((true, s.to_string())),
        };

        let removal = {
            let conn = self.db_conn.lock().await;
            block_in_place(|| {
                unsubscribe_channel_to_section_by_subj_number(
                    &conn,
                    interaction.channel_id.get(),
                    term,
                    subj,
                    course,
                    sec,
                    all_spans,
                )
            })
        };

        match removal {
            Ok(0) => Ok((
                true,
                "Could not find subscription matching given query".to_string(),
            )),
            Ok(1) => Ok((false, "Unsubscribed from 1 section".to_string())),
            Ok(n) => Ok((false, format!("Unsubscribed from {n} sections"))),
            Err(e) => {
                log::error!("Unexpected error attempting to insert new subscription: {e:?}");
                Err(e.into())
            }
        }
    }

    async fn list_subscriptions(&self, ctx: Context, interaction: CommandInteraction) {
        let defer = Self::defer_bg(&ctx, &interaction, true);
        log::debug!("Listing subscriptions for {}", interaction.channel_id.get());

        let page = match interaction.data.options.first() {
            None => 1,
            Some(CommandDataOption {
                name: "page",
                value: CommandDataOptionValue::Integer(p),
                ..
            }) if *p > 0 => *p as u64,
            Some(o) => {
                log::error!("Unknown argument passed to list command: {o:?}");
                interaction
                    .edit_response(
                        &ctx,
                        EditInteractionResponse::new()
                            .content("_Internal error while parsing command arguments_"),
                    )
                    .await
                    .unwrap();
                return;
            }
        };

        let subs_res = {
            let conn = self.db_conn.lock().await;
            block_in_place(|| {
                subscriptions_with_latest_description_for_channel(
                    &conn,
                    interaction.channel_id.get(),
                    page,
                )
            })
        };

        let subs = match subs_res {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "Database error while looking up subscription details for channel {}: {e}",
                    interaction.channel_id.get()
                );
                interaction
                    .edit_response(
                        &ctx,
                        EditInteractionResponse::new()
                            .content("_Internal error while looking up subscription details_"),
                    )
                    .await
                    .unwrap();
                return;
            }
        };

        let msg = match (subs.0, subs.1.as_slice()) {
            (0, _) => {
                EditInteractionResponse::new().content("This channel has no section subscriptions")
            }
            (pages, &[]) => EditInteractionResponse::new()
                .content(format!("There are only {pages} pages available")),
            (pages, _) => {
                let channel_name = interaction
                    .channel
                    .as_ref()
                    .and_then(|c| c.name.as_deref())
                    .unwrap_or("this channel");
                let embed = CreateEmbed::new()
                    .description(subs.1.join("\n"))
                    .color(Colour::RED)
                    .footer(
                        CreateEmbedFooter::new(format!("{page} / {pages}"))
                            .icon_url(MARTLET_PNG_URL),
                    )
                    .title(format!("Watched sections for {channel_name}"));
                EditInteractionResponse::new().embed(embed)
            }
        };

        defer.await.unwrap();
        interaction.edit_response(&ctx, msg).await.unwrap();
        log::trace!("Finished rendering and sending subscription list");
    }

    async fn lookup_course_wrapper(&self, ctx: Context, interaction: CommandInteraction) {
        let defer_handle = Self::defer_bg(&ctx, &interaction, true);
        let res = block_in_place(|| self.lookup_course(&interaction));
        defer_handle.await.unwrap();
        let resp = res.unwrap_or_else(|msg| EditInteractionResponse::new().content(msg));
        interaction.edit_response(&ctx, resp).await.unwrap();
    }

    fn lookup_course(
        &self,
        interaction: &CommandInteraction,
    ) -> Result<EditInteractionResponse, &'static str> {
        let mut term = None;
        let mut crn = None;
        let mut subject = None;
        let mut number = None;
        let mut section = None;
        let mut all_spans = false;

        for option in interaction.data.options() {
            match (option.name, option.value) {
                ("term", ResolvedValue::String(val)) => {
                    let res = {
                        let conn = self.db_conn.blocking_lock();
                        parse_term(&conn, val)
                    };
                    match res {
                        Ok(Some(t)) => term = Some(t),
                        Ok(None) => return Err("Unknown term number or name"),
                        Err(e) => {
                            log::error!("Error looking up term for {val}: {e}");
                            return Err("_Internal database error while looking up term_");
                        }
                    }
                }
                ("crn", ResolvedValue::Integer(val)) => crn = Some(val as u32),
                ("subject", ResolvedValue::String(val)) => {
                    subject = Some(
                        Subject::try_from(val)
                            .map_err(|_| "Subject argument was not 4 characters")?,
                    );
                }
                ("number", ResolvedValue::String(val)) => {
                    number = Some(
                        CourseNumber::try_from(val.to_owned())
                            .map_err(|_| "Could not parse course number")?,
                    );
                }
                ("section", ResolvedValue::String(val)) => {
                    section = Some(
                        SectionNumber::try_from(val.to_owned())
                            .map_err(|_| "Could not parse section number")?,
                    )
                }
                ("allspans", ResolvedValue::Boolean(val)) => {
                    all_spans = val;
                }
                (k, v) => {
                    log::error!("Unexpected argument or type for lookup command: {k} {v:?}");
                    return Err("_Internal error parsing command arguments_");
                }
            }
        }

        let records = {
            let conn = self.db_conn.blocking_lock();
            lookup_latest_sections_with_term_name(
                &conn, term, crn, subject, number, section, 10, all_spans,
            )
        };

        match records {
            Ok(v) if v.is_empty() => Err("No results found for query"),
            Ok(v) => Ok(EditInteractionResponse::new().add_embeds(
                v.into_iter()
                    .map(|(sr, term_name)| create_section_update_embed(None, sr, &term_name, true))
                    .collect(),
            )),
            Err(e) => {
                log::error!("Error executing lookup query: {e}");
                Err("_Internal database error looking up requested sections_")
            }
        }
    }

    async fn status(&self, ctx: Context, interaction: CommandInteraction) {
        let defer_handle = Self::defer_bg(&ctx, &interaction, true);

        let counts = block_in_place(|| {
            let conn = self.db_conn.blocking_lock();
            db_stats(&conn)
        });

        defer_handle.await.unwrap();
        match counts {
            Ok(c) => {
                let embed = CreateEmbed::new()
                    .color(Colour::RED)
                    .footer(CreateEmbedFooter::new("").icon_url(MARTLET_PNG_URL))
                    .author(CreateEmbedAuthor::new(format!(
                        "{} {}",
                        env!("CARGO_BIN_NAME"),
                        env!("CARGO_PKG_VERSION")
                    )))
                    .fields([
                        (
                            "Lat update",
                            format!("<t:{}>", c.last_update.timestamp()),
                            true,
                        ),
                        ("Sections Tracked", c.sections.to_string(), true),
                        ("Historical Entries", c.section_records.to_string(), true),
                        ("Terms", c.terms.to_string(), true),
                        ("Subscriptions", c.subs.to_string(), true),
                        ("Channels", c.channels.to_string(), true),
                    ]);
                interaction
                    .edit_response(&ctx, EditInteractionResponse::new().embed(embed))
                    .await
                    .unwrap();
            }
            Err(e) => {
                log::error!("Failed to get database counts: {e}");
                interaction
                    .edit_response(
                        &ctx,
                        EditInteractionResponse::new()
                            .content("_Internal database error while processing command_"),
                    )
                    .await
                    .unwrap();
            }
        }
    }
}

#[tokio::main]
pub async fn discord_thread_entry(
    discord_token: String,
    db_path: PathBuf,
    receiver: UnboundedReceiver<SectionChangeMessage>,
) {
    // Panics in this thread kill the whole process
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        std::process::abort();
    }));

    let conn = open_or_init(db_path, false).unwrap();

    let handler = Handler {
        db_conn: Mutex::new(conn),
        receiver: Mutex::new(Some(receiver)),
        receiver_started: AtomicBool::new(false),
        subscription_list_cache: RwLock::new(HashMap::new()),
    };
    let mut client = Client::builder(discord_token, GatewayIntents::GUILD_MESSAGES)
        .event_handler(handler)
        // .type_map_insert::<SubscriptionListCache>(Default::default())
        .await
        .expect("Error creating client");

    client.start().await.unwrap();
}
