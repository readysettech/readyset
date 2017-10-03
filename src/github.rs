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

    fn get_client(&self) -> Github {
        Github::new(self.api_token.clone()).unwrap()
    }

    pub fn notify_pending(&self, push: &Push, commit: &Commit) -> Result<(), String> {
        let payload = Payload {
            state: String::from("pending"),
            description: String::from("Currently tasting..."),
            context: String::from("Taster"),
        };

        let result = self.get_client()
            .post(&payload)
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
            state: state.to_string(),
            description: format!("It {}.", taste),
            context: "Taster".to_string(),
        };

        // TODO: move this to a method
        let result = self.get_client()
            .post(&payload)
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
}
