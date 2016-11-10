use slack_hook::{PayloadBuilder, Slack, SlackTextContent, SlackLink};
use slack_hook::SlackTextContent::{Text, Link};

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

    pub fn notify(&self, commit_id: &str) -> Result<(), String> {
        let payload = PayloadBuilder::new()
            .text(vec![Text("I tasted ".into()),
                       Link(SlackLink::new(&format!("{}/commit/{}",
                                                    self.github_repo,
                                                    commit_id),
                                           commit_id)),
                       Text("!".into())]
                .as_slice())
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
