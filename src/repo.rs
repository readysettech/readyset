use git2;
use git2::{AutotagOption, ErrorCode, FetchOptions, Oid, RemoteCallbacks, Repository, ResetType};
use std::fs;
use std::path::Path;

pub struct Workspace {
    pub path: String,
    pub repo: Repository,
    pub remote_url: String,
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
            remote_url: String::from(github_repo),
        }
    }

    pub fn fetch(&self) -> Result<(), git2::Error> {
        let refspec = "refs/heads/*:refs/heads/*";
        let cb = RemoteCallbacks::new();
        let mut remote = try!(self.repo.remote_anonymous(&self.remote_url));
        let mut opts = FetchOptions::new();
        opts.remote_callbacks(cb)
            .download_tags(AutotagOption::All);

        try!(remote.fetch(&[refspec], Some(&mut opts), None));
        Ok(())
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
