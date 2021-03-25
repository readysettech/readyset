use slack_hook::SlackTextContent::{Link, Text};
use slack_hook::{
    Attachment, AttachmentBuilder, Field, PayloadBuilder, Slack, SlackLink, SlackText,
};

use crate::config::Config;
use crate::taste::{BenchmarkResult, TastingResult};
use crate::Push;

pub struct SlackNotifier {
    conn: Slack,
    channel: String,
    verbose: bool,
}

impl SlackNotifier {
    pub fn new(hook_url: &str, channel: &str, _repo_url: &str, verbose: bool) -> SlackNotifier {
        SlackNotifier {
            conn: Slack::new(hook_url).unwrap(),
            channel: String::from(channel),
            verbose,
        }
    }

    pub fn notify(
        &self,
        cfg: Option<&Config>,
        res: &TastingResult,
        push: &Push,
    ) -> Result<(), String> {
        let mut text = vec![
            Text("I've tasted _".into()),
            Text(format!("\"{}\"_ (", push.head_commit.msg.lines().next().unwrap()).into()),
            Link(SlackLink::new(
                &push.head_commit.url,
                &format!("{}", push.head_commit.id)[0..6],
            )),
        ];
        match push.pusher {
            Some(ref p) => {
                let alias = match cfg {
                    Some(cfg) => match cfg.slack_aliases.get(p) {
                        None => p,
                        Some(a) => a,
                    },
                    None => p,
                };
                text.push(Text(format!("), pushed by @{}", alias).into()))
            }
            None => text.push(Text(")".into())),
        }
        if let Some(ref b) = res.branch {
            text.push(Text(format!("to *{}*", b).into()))
        }
        let payload = PayloadBuilder::new()
            .text(text.as_slice())
            .attachments(self.result_to_attachments(&res))
            .channel(self.channel.clone())
            .username("taster")
            .icon_emoji(":tea:")
            .link_names(true)
            .build()
            .unwrap();

        match self.conn.send(&payload) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("{:?}", e)),
        }
    }

    fn result_to_attachments(&self, res: &TastingResult) -> Vec<Attachment> {
        let color = if !res.build || !res.test || !res.bench {
            "danger"
        } else {
            "good"
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
                out.push_str(":heavy_check_mark:");
            } else {
                out.push_str(":x:");
            }
            out
        };

        let mut attachments = Vec::new();
        let build_att = AttachmentBuilder::new("")
            .title(format!("It {}.", taste))
            .text(format!(
                "{} {} {}",
                check("Build", res.build),
                check("Tests", res.test),
                check("Benchmarks", res.bench)
            ))
            .color(color)
            .build()
            .unwrap();
        attachments.push(build_att);

        let is_regression =
            |(_, v): (_, &BenchmarkResult<f64>)| matches!(v, BenchmarkResult::Regression(_, _));
        let is_neutral =
            |(_, v): (_, &BenchmarkResult<f64>)| matches!(v, BenchmarkResult::Neutral(_, _));

        if let Some(r) = &res.results {
            for (bm, status, res) in r {
                if !status.success() {
                    let att = AttachmentBuilder::new("")
                        .color("danger")
                        .title(format!("{} failed!", bm.name))
                        .build()
                        .unwrap();
                    attachments.push(att);
                    continue;
                }

                let mut fields = res
                    .iter()
                    .filter(|r| self.verbose || (is_regression(*r) && r.1.change() != 0.0))
                    .map(|(k, v)| {
                        let icon = if v.change() > 0.1 {
                            ":chart_with_upwards_trend:"
                        } else if v.change() < -0.1 {
                            ":chart_with_downwards_trend:"
                        } else {
                            ""
                        };
                        Field {
                            title: k.clone(),
                            value: SlackText::new(format!(
                                "{} {} ({:+.2}%)",
                                icon,
                                v.value(),
                                v.change() * 100.0
                            )),
                            short: Some(true),
                        }
                    })
                    .collect::<Vec<_>>();
                fields.sort_unstable_by(|x, y| x.title.cmp(&y.title));

                let color = if res.iter().all(&is_regression) {
                    // red
                    "danger"
                } else if res.iter().any(&is_regression) {
                    // amber
                    "warning"
                } else if res.iter().all(&is_neutral) {
                    // default, gray
                    ""
                } else {
                    // green
                    "good"
                };

                if self.verbose || !res.iter().all(&is_neutral) {
                    let att = AttachmentBuilder::new("")
                        .color(color)
                        .fields(fields)
                        .build()
                        .unwrap();
                    attachments.push(att);
                }
            }
        }
        attachments
    }
}
