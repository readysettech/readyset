use slack_hook::{Attachment, AttachmentBuilder, PayloadBuilder, Slack, SlackLink};
use slack_hook::SlackTextContent::{Text, Link};

use taste::TastingResult;

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

    pub fn notify(&self, res: TastingResult) -> Result<(), String> {
        let payload = PayloadBuilder::new()
            .text(vec![Text("I've tasted ".into()),
                       Link(SlackLink::new(&format!("{}/commit/{}",
                                                    self.github_repo,
                                                    res.commit_id),
                                           &res.commit_id)),
                       Text(format!("-- `{}`", res.commit_msg.lines().next().unwrap()).into())]
                .as_slice())
            .attachments(vec![result_to_attachment(&res)])
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

fn result_to_attachment(res: &TastingResult) -> Attachment {
    let color = if !res.build || !res.bench {
        "danger"
    } else {
        "good"
    };

    let title = if !res.build {
        "Build failure!"
    } else if !res.bench {
        "Benchmark failure!"
    } else {
        "Performance results:"
    };

    let taste = if !res.build || !res.bench {
        "was inedible"
    } else {
        "tasted nice"
    };

    AttachmentBuilder::new(format!("It {}.", taste))
        .color(color)
        .title(title)
        .build()
        .unwrap()
}
