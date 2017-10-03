use Push;
use Commit;
use config::Config;
use taste::{TastingResult};
use github_rs::client::{Executor, Github};
use serde_json::Value;

pub struct GithubNotifier {
    api_token: String,
}

#[derive(Serialize)]
struct Payload {
    state: String,
    description: String,
    context: String,
}

impl GithubNotifier {
    pub fn new(api_token: &str) -> GithubNotifier {
        GithubNotifier { api_token: String::from(api_token) }
    }

    fn post_status(&self, push: &Push, commit: &Commit, payload: &Payload) -> Result<(), String> {
        let client = Github::new(self.api_token.clone()).unwrap();
        let result = client
            .post(payload)
            .repos()
            .owner(push.owner_name.clone().unwrap().as_str())
            .repo(push.repo_name.clone().unwrap().as_str())
            .statuses()
            .sha(&commit.id.to_string())
            .execute::<Value>();

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    pub fn notify_pending(&self, push: &Push, commit: &Commit) -> Result<(), String> {
        let payload = Payload {
            context: "Taster".to_string(),
            state: "pending".to_string(),
            description: "Currently tasting...".to_string(),
        };

        self.post_status(push, commit, &payload)
    }

    pub fn notify(
        &self,
        _cfg: Option<&Config>,
        res: &TastingResult,
        push: &Push,
        commit: &Commit,
    ) -> Result<(), String> {
        let state = if !res.build || !res.test || !res.bench {
            "failure"
        } else {
            "success"
        };

        let taste = if !res.build || !res.bench {
            "was inedible"
        } else if !res.test {
            "had a mixed palate"
        } else {
            "tasted nice"
        };

        let payload = Payload {
            context: "Taster".to_string(),
            state: state.to_string(),
            description: format!("It {}.", taste),
        };

        self.post_status(push, commit, &payload)
    }
}
