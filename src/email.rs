use lettre::email::EmailBuilder;
use lettre::transport::smtp::SmtpTransportBuilder;
use lettre::transport::EmailTransport;

use taste::TastingResult;

pub struct EmailNotifier {
    // mailer: SmtpTransport,
    addr: String,
    github_repo: String,
}

impl EmailNotifier {
    pub fn new(addr: &str, repo_url: &str) -> EmailNotifier {
        EmailNotifier {
            // mailer: SmtpTransportBuilder::localhost().unwrap().build(),
            addr: String::from(addr),
            github_repo: String::from(repo_url),
        }
    }

    pub fn notify(&self, res: &TastingResult) -> Result<(), String> {
        // Unfortunately, we have to construct the transport here, since hyper forces us to accept
        // &self rather than &mut self, so we can't store the mailer in the `EmailNotifier` struct
        let mut mailer = SmtpTransportBuilder::localhost().unwrap().build();

        let email = EmailBuilder::new()
            .to(self.addr.as_str())
            .from("taster@tbilisi.csail.mit.edu")
            .body(&format!("Hello world from {}", self.github_repo))
            .subject(&format!("[taster] Result for {}", res.commit_id))
            .build()
            .unwrap();

        match mailer.send(email) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("{:?}", e)),
        }
    }
}
