use serenity::all::{
    ChannelId, Colour, Context, CreateEmbed, CreateEmbedAuthor, CreateEmbedFooter, CreateMessage,
    ModelError,
};

use tokio::sync::mpsc::UnboundedReceiver;

use crate::SectionChangeMessage;
use crate::db::SectionRecord;
use crate::discord::MARTLET_PNG_URL;
use crate::minerva::parser::Availability;

pub fn create_section_update_embed(
    prev: Option<SectionRecord>,
    next: SectionRecord,
    term_name: &str,
    skip_update_message: bool,
) -> CreateEmbed {
    let mut desc = String::new();

    let SectionRecord {
        term: _,
        timestamp: nt,
        inner: nd,
    } = next;
    let na = nd.availability();

    let mut builder = CreateEmbed::new()
        .color(Colour::RED)
        .author(
            CreateEmbedAuthor::new(format!("{} {}-{}", nd.subject, nd.number, nd.section))
                .url("https://horizon.mcgill.ca/pban1/bwskfreg.P_AltPin"),
        )
        .footer(
            CreateEmbedFooter::new("Click the course name for Quick Add/Drop")
                .icon_url(MARTLET_PNG_URL),
        )
        .field("Term", term_name, true)
        .field("CRN", nd.crn.to_string(), true);

    // Fields assumed immutable: subject, number, section, title, section type

    if let Some(SectionRecord {
        timestamp: pt,
        inner: pd,
        ..
    }) = prev
    {
        if !skip_update_message {
            desc.push_str("Section updated");
        }

        // Order of importance for fields: Subj-Number-Section, SecType, Title,
        // then Status, WL Rem, Rem, Instructor, Loc, then rest
        if nd.subject != pd.subject {
            builder = builder.field(
                "Subject",
                format!("{} (was {})", nd.subject, pd.subject),
                false,
            );
        }
        if nd.number != pd.number {
            builder = builder.field(
                "Number",
                format!("{} (was {})", nd.number, pd.number),
                false,
            );
        }
        if nd.section != pd.section {
            builder = builder.field(
                "Section",
                format!("{} (was {})", nd.section, pd.section),
                false,
            );
        }
        if nd.sec_type != pd.sec_type {
            builder = builder.field(
                "Section Type",
                format!("{} (was {})", nd.sec_type, pd.sec_type),
                false,
            );
        }
        if nd.title != pd.title {
            builder = builder.field("Title", format!("{} (was {})", nd.title, pd.title), false);
        }
        if nd.status != pd.status {
            builder = builder.field(
                "Status",
                format!("{} (was {})", nd.status, pd.status),
                false,
            );
        }
        if nd.wl_remaining != pd.wl_remaining {
            builder = builder.field(
                "Waitlist Remaining",
                format!("{} (was {})", nd.wl_remaining, pd.wl_remaining),
                false,
            );
        }
        if nd.wl_actual != pd.wl_actual {
            builder = builder.field(
                "Waitlist Actual",
                format!("{} (was {})", nd.wl_actual, pd.wl_actual),
                true,
            );
        }
        if nd.wl_capacity != pd.wl_capacity {
            builder = builder.field(
                "Waitlist Remaining",
                format!("{} (was {})", nd.wl_capacity, pd.wl_capacity),
                true,
            );
        }
        if nd.remaining != pd.remaining {
            builder = builder.field(
                "Seats Remaining",
                format!("{} (was {})", nd.remaining, pd.remaining),
                false,
            );
        }
        if nd.actual != pd.actual {
            builder = builder.field(
                "Seats Actual",
                format!("{} (was {})", nd.actual, pd.actual),
                true,
            );
        }
        if nd.capacity != pd.capacity {
            builder = builder.field(
                "Seats Capacity",
                format!("{} (was {})", nd.capacity, pd.capacity),
                true,
            );
        }
        if nd.instructor != pd.instructor {
            builder = builder.field(
                "Instructor",
                format!(
                    "{} (was {})",
                    nd.instructor.as_deref().unwrap_or("TBA"),
                    pd.instructor.as_deref().unwrap_or("TBA")
                ),
                false,
            );
        }
        if nd.location != pd.location {
            builder = builder.field(
                "Location",
                format!(
                    "{} (was {})",
                    nd.location.as_deref().unwrap_or("TBA"),
                    pd.location.as_deref().unwrap_or("TBA")
                ),
                false,
            );
        }
        if nd.millicredits != pd.millicredits {
            builder = builder.field(
                "Credits",
                format!("{} (was {})", nd.millicredits, pd.millicredits),
                false,
            );
        }
        if nd.days != pd.days {
            builder = builder.field("Credits", format!("{} (was {})", nd.days, pd.days), false);
        }
        if nd.time != pd.time {
            builder = builder.field(
                "Time",
                format!(
                    "{} (was {})",
                    nd.time.as_deref().unwrap_or("TBA"),
                    pd.time.as_deref().unwrap_or("TBA")
                ),
                false,
            )
        }
        if nd.date != pd.date {
            builder = builder.field("Dates", format!("{} (was {})", nd.date, pd.date), false)
        }
        if nd.notes != pd.notes {
            builder = builder.field(
                "Notes",
                format!("{}\n\nWas:\n{}", nd.notes.join("\n"), pd.notes.join("\n")),
                false,
            );
        }
        builder = builder.field(
            "Last Checked",
            format!("<t:{}> (was <t:{}>)", nt.timestamp(), pt.timestamp()),
            false,
        );
    } else {
        if !skip_update_message {
            desc.push_str("New section found");
        }

        // Do normal order for an added course
        builder = builder.field("Credits", nd.millicredits.to_string(), false);
        builder = builder.field("Days", nd.days.to_string(), false);
        builder = builder.field("Time", nd.time.unwrap_or("TBA".to_string()), false);
        builder = builder.field("Seats Remaining", nd.remaining.to_string(), false);
        builder = builder.field("Seats Actual", nd.actual.to_string(), true);
        builder = builder.field("Seats Capacity", nd.capacity.to_string(), true);
        builder = builder.field("Waitlist Remaining", nd.wl_remaining.to_string(), false);
        builder = builder.field("Waitlist Actual", nd.wl_actual.to_string(), true);
        builder = builder.field("Waitlist Capacity", nd.wl_capacity.to_string(), true);
        builder = builder.field(
            "Instructor",
            nd.instructor.as_deref().unwrap_or("TBA"),
            false,
        );
        builder = builder.field("Date", nd.date, false);
        builder = builder.field("Location", nd.location.as_deref().unwrap_or("TBA"), false);
        builder = builder.field("Status", nd.status.to_string(), false);
        builder = builder.field("Notes", nd.notes.join("\n"), false);
        builder = builder.field("Last Checked", format!("<t:{}>", nt.timestamp()), false);
    }

    if !skip_update_message {
        desc.push('\n');
    }
    desc.push_str(na.to_string().as_str());

    builder.description(desc)
}

pub async fn section_change_listener(
    ctx: Context,
    mut receiver: UnboundedReceiver<SectionChangeMessage>,
) {
    log::debug!("Started section change listener thread");
    loop {
        // Unwrap because the channel should never close
        let msg = receiver.recv().await.unwrap();

        let pa = msg.prev.as_ref().map(|pr| pr.inner.availability());
        let na = msg.next.inner.availability();
        let is_availability = match (pa, na) {
            (_, Availability::Full | Availability::WaitlistFull) => false,
            (Some(p), n) => p != n,
            (None, _) => true,
        };

        log::trace!(
            "Received section change (av: {is_availability}) message about {}-{} for {:?}",
            msg.next.term,
            msg.next.inner.crn,
            msg.channels
        );

        let channels = msg
            .channels
            .into_iter()
            .filter(|(_, all)| *all || is_availability)
            .map(|(c, _)| c)
            .collect::<Vec<u64>>();

        let embed = create_section_update_embed(msg.prev, msg.next, msg.term_name.as_str(), false);
        let message = CreateMessage::new().add_embed(embed);

        for channel in channels {
            match ChannelId::new(channel)
                .send_message(&ctx, message.clone())
                .await
            {
                Ok(_) => {}
                Err(serenity::Error::Model(ModelError::InvalidPermissions { .. })) => {
                    log::error!(
                        "Missing permissions to send to channel {channel}, removing subscriptions"
                    );
                    // TODO ?
                    // remove_subs(&db, channel);
                }
                e => {
                    log::error!("Failed to send message to {channel}: {e:?}");
                }
            }
        }
    }
}
