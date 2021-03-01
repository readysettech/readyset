use config::Config;
use github_rs::client::{Executor, Github};
use github_rs::StatusCode;
use serde_json;
use taste::TastingResult;
use Commit;
use Push;

pub struct GithubNotifier {
    api_token: String,
}

#[derive(Deserialize)]
struct GithubError {
    message: String,
}

#[derive(Serialize)]
struct Payload {
    state: String,
    description: String,
    context: String,
}

impl GithubNotifier {
    pub fn new(api_token: &str) -> GithubNotifier {
        GithubNotifier {
            api_token: String::from(api_token),
        }
    }

    fn post_status(&self, push: &Push, commit: &Commit, payload: Payload) -> Result<(), String> {
        let owner_name = push.owner_name.clone().unwrap();
        let repo_name = push.repo_name.clone().unwrap();
        println!(
            "Setting status of {}/{}#{} to {}",
            owner_name, repo_name, commit.id, payload.state
        );

        let client = Github::new(self.api_token.clone()).unwrap();
        let result = client
            .post(&payload)
            .repos()
            .owner(owner_name.as_str())
            .repo(repo_name.as_str())
            .statuses()
            .sha(&commit.id.to_string())
            .execute::<serde_json::Value>();

        match result {
            Ok((_, StatusCode::CREATED, Some(_))) => Ok(()),
            Ok((_, status_code, Some(response))) => serde_json::from_value::<GithubError>(response)
                .map_err(|err| format!("Failed to parse error response ({}): {}", status_code, err))
                .and_then(|err| Err(err.message.into())),
            Ok((_, _, None)) => Err("Received error response from GitHub with no message".into()),
            Err(err) => Err(format!("Failed to execute GitHub request: {}", err)),
        }
    }

    pub fn notify_pending(&self, push: &Push, commit: &Commit) -> Result<(), String> {
        let payload = Payload {
            context: "Taster".to_string(),
            state: "pending".to_string(),
            description: "Currently tasting...".to_string(),
        };

        self.post_status(push, commit, payload)
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

        self.post_status(push, commit, payload)
    }
}
