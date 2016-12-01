use slack_hook::{Attachment, AttachmentBuilder, Field, PayloadBuilder, Slack, SlackText, SlackLink};
use slack_hook::SlackTextContent::{Text, Link};

use taste::{BenchmarkResult, TastingResult};

pub struct SlackNotifier {
    conn: Slack,
    channel: String,
    github_repo: String,
}

impl SlackNotifier {
    pub fn new(hook_url: &str, channel: &str, repo_url: &str) -> SlackNotifier {
        SlackNotifier {
            conn: Slack::new(hook_url).unwrap(),
            channel: String::from(channel),
            github_repo: String::from(repo_url),
        }
    }

    pub fn notify(&self, res: &TastingResult) -> Result<(), String> {
        let payload = PayloadBuilder::new()
            .text(vec![Text("I've tasted commit _".into()),
                       Text(format!("\"{}\"_ (", res.commit_msg.lines().next().unwrap()).into()),
                       Link(SlackLink::new(&res.commit_url, &res.commit_id[0..6])),
                       Text(format!(") from branch *{}*", res.branch).into())]
                .as_slice())
            .attachments(result_to_attachments(&res))
            .channel(self.channel.clone())
            .username("taster")
            .icon_emoji(":tea:")
            .build()
            .unwrap();

        match self.conn.send(&payload) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("{:?}", e)),
        }
    }
}

fn result_to_attachments(res: &TastingResult) -> Vec<Attachment> {
    let color = if !res.build || !res.test || !res.bench {
        "danger"
    } else {
        "good"
    };

    let title = if !res.build {
        "Build failure!"
    } else if !res.test {
        "Test failure!"
    } else if !res.bench {
        "Benchmark failure!"
    } else {
        "Performance results:"
    };

    let taste = if !res.build || !res.bench {
        "was inedible"
    } else if !res.test {
        "had a mixed palate"
    } else {
        "tasted nice"
    };

    let check = |title: &str, result: bool| {
        let mut out = format!("{}: ", title);
        if result {
            out.push_str(":white_check_mark:");
        } else {
            out.push_str(":x:");
        }
        out
    };

    let mut attachments = Vec::new();
    let build_att = AttachmentBuilder::new("")
        .title(format!("It {}.", taste))
        .text(format!("{} {} {}",
                      check("Build", res.build),
                      check("Tests", res.test),
                      check("Benchmark", res.bench)))
        .color(color)
        .build()
        .unwrap();
    attachments.push(build_att);

    match res.results {
        None => (),
        Some(ref r) => {
            for res in r {
                let mut nv = res.iter()
                    .map(|(k, v)| {
                        let val = match v {
                            &BenchmarkResult::Neutral(ref s, _) => s,
                            _ => unimplemented!(),
                        };
                        Field {
                            title: k.clone(),
                            value: SlackText::new(val.clone()),
                            short: Some(true),
                        }
                    })
                    .collect::<Vec<_>>();
                nv.sort_by(|a, b| b.title.cmp(&a.title));
                let att = AttachmentBuilder::new("")
                    .color(color)
                    .fields(nv)
                    .build()
                    .unwrap();
                attachments.push(att);
            }
        }
    }
    attachments
}
