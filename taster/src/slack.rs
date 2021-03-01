use slack_hook::SlackTextContent::{Link, Text};
use slack_hook::{
    Attachment, AttachmentBuilder, Field, PayloadBuilder, Slack, SlackLink, SlackText,
};

use config::Config;
use taste::{BenchmarkResult, TastingResult};
use Push;

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
            verbose: verbose,
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
            None => text.push(Text(format!(")").into())),
        }
        match res.branch {
            Some(ref b) => text.push(Text(format!("to *{}*", b).into())),
            None => (),
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

        let is_regression = |(_, v): (_, &BenchmarkResult<f64>)| match *v {
            BenchmarkResult::Regression(_, _) => true,
            _ => false,
        };
        let is_neutral = |(_, v): (_, &BenchmarkResult<f64>)| match *v {
            BenchmarkResult::Neutral(_, _) => true,
            _ => false,
        };

        match res.results {
            None => (),
            Some(ref r) => {
                for &(ref bm, ref status, ref res) in r {
                    if !status.success() {
                        let att = AttachmentBuilder::new("")
                            .color("danger")
                            .title(format!("{} failed!", bm.name))
                            .build()
                            .unwrap();
                        attachments.push(att);
                        continue;
                    }

                    let mut nv = res
                        .iter()
                        .map(|(k, v)| {
                            let val = match *v {
                                BenchmarkResult::Improvement(ref s, ref p) => (s, p),
                                BenchmarkResult::Neutral(ref s, ref p) => (s, p),
                                BenchmarkResult::Regression(ref s, ref p) => (s, p),
                            };
                            let icon = if *val.1 > 0.1 {
                                ":chart_with_upwards_trend:"
                            } else if *val.1 < -0.1 {
                                ":chart_with_downwards_trend:"
                            } else {
                                ""
                            };
                            Field {
                                title: k.clone(),
                                value: SlackText::new(format!(
                                    "{} {} ({:+.2}%)",
                                    icon,
                                    val.0,
                                    val.1 * 100.0
                                )),
                                short: Some(true),
                            }
                        })
                        .collect::<Vec<_>>();
                    nv.sort_by(|a, b| b.title.cmp(&a.title));

                    let col = if res.iter().all(&is_regression) {
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
                            .color(col)
                            .fields(nv)
                            .build()
                            .unwrap();
                        attachments.push(att);
                    }
                }
            }
        }
        attachments
    }
}
