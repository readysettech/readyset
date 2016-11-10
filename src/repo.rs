use git2::{ErrorCode, Oid, Repository, ResetType};
use std::fs;
use std::path::Path;

pub struct Workspace {
    pub path: String,
    pub repo: Repository,
}

impl Workspace {
    pub fn new(github_repo: &str, local_path: &Path) -> Workspace {
        // Make workdir if it doesn't exist
        if !local_path.is_dir() {
            fs::create_dir_all(&local_path)
                .expect(format!("Couldn't mkdir {}", local_path.display()).as_str());
        }

        let repo = match Repository::open(local_path.to_str().unwrap()) {
            Ok(r) => r,
            Err(e) => {
                if e.code() == ErrorCode::NotFound {
                    // Repo does not exist, so let's clone into the workdir
                    println!("Cloning '{}' into local workspace at '{}'...",
                             github_repo,
                             local_path.to_str().unwrap());
                    match Repository::clone(github_repo, local_path.to_str().unwrap()) {
                        Ok(r) => r,
                        Err(e) => panic!(e),
                    }
                } else {
                    panic!(e);
                }
            }
        };

        Workspace {
            path: String::from(local_path.to_str().unwrap()),
            repo: repo,
        }
    }

    pub fn checkout_commit(&self, commit_id: &str) {
        // N.B.: this crashes hard if the commit is invalid (e.g., because workdir contains the
        // wrong repo)
        let c = self.repo.find_object(Oid::from_str(commit_id).unwrap(), None).unwrap();
        match self.repo.reset(&c, ResetType::Hard, None) {
            Ok(_) => println!("Checked out {}", commit_id),
            Err(e) => println!("Failed to check out {}: {}", commit_id, e.message()),
        };
    }
}
