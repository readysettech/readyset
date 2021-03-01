use auth::with_authentication;

use git2;
use git2::{
    AutotagOption, BranchType, Commit, ErrorCode, FetchOptions, RemoteCallbacks, Repository,
    ResetType,
};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub struct Workspace {
    pub path: String,
    pub repo: Repository,
    pub remote_url: String,
}

fn clone(url: &str, path: &Path) -> Result<Repository, git2::Error> {
    let cfg = git2::Config::new().unwrap();

    with_authentication(url, &cfg, |f| {
        let mut cb = RemoteCallbacks::new();
        cb.credentials(f);
        let mut opts = FetchOptions::new();
        opts.remote_callbacks(cb);
        let mut rb = git2::build::RepoBuilder::new();
        rb.fetch_options(opts);
        rb.clone(url, path)
    })
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
                    println!(
                        "Cloning '{}' into local workspace at '{}'...",
                        github_repo,
                        local_path.to_str().unwrap()
                    );
                    clone(github_repo, local_path).unwrap()
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

    pub fn branch_heads(&self) -> HashMap<String, Commit> {
        match self.fetch() {
            Err(e) => panic!("{:#?}", e),
            Ok(()) => (),
        }
        match self.repo.branches(Some(BranchType::Remote)) {
            Err(e) => panic!("Couldn't get remote branches: {}", e.message()),
            Ok(br) => br
                .map(|b| {
                    let branch = b.unwrap().0;
                    (
                        String::from(branch.name().as_ref().unwrap().unwrap()),
                        self.repo
                            .find_commit(branch.get().target().unwrap())
                            .unwrap(),
                    )
                })
                .collect(),
        }
    }

    pub fn fetch(&self) -> Result<(), git2::Error> {
        let refspec = "+refs/heads/*:refs/remotes/origin/*";

        with_authentication(&self.remote_url, &try!(self.repo.config()), |f| {
            let mut cb = RemoteCallbacks::new();
            cb.credentials(f);
            let mut remote = try!(self.repo.remote_anonymous(&self.remote_url));
            let mut opts = FetchOptions::new();
            opts.remote_callbacks(cb).download_tags(AutotagOption::All);

            try!(remote.fetch(&[refspec], Some(&mut opts), None));
            Ok(())
        })
    }

    pub fn checkout_commit(&self, commit_id: &git2::Oid) -> Result<(), String> {
        use std::error::Error;
        // N.B.: this will turn into a no-op if the workdir contains the wrong
        // repo, as the commit won't exist
        let c = match self.repo.find_object(*commit_id, None) {
            Err(e) => return Err(String::from(e.description())),
            Ok(o) => o,
        };
        match self.repo.reset(&c, ResetType::Hard, None) {
            Ok(_) => println!("Checked out {}", commit_id),
            Err(e) => println!("Failed to check out {}: {}", commit_id, e.message()),
        };
        Ok(())
    }
}
