#[cfg(feature="web")]
extern crate distributary;
#[cfg(feature="web")]
extern crate shortcut;

#[cfg(feature="web")]
fn main() {
    use distributary::*;

    // set up graph
    let mut g = distributary::FlowGraph::new();

    {
        let mut inc = SqlIncorporator::new(&mut g);

        // add article base node
        let _article = "INSERT INTO article (id, user, title, url) VALUES (?, ?, ?, ?);"
            .to_flow_parts(&mut inc, None)
            .unwrap();

        // add vote base table
        let _vote = "INSERT INTO vote (user, id) VALUES (?, ?);"
            .to_flow_parts(&mut inc, None)
            .unwrap();

        // add a user account base table
        let _user = "INSERT INTO user (id, username, hash) VALUES (?, ?, ?);"
            .to_flow_parts(&mut inc, None)
            .unwrap();

        // add vote count
        let vc = "SELECT vote.id, COUNT(vote.user) AS votes FROM vote;"
            .to_flow_parts(&mut inc, Some("vc".into()))
            .unwrap();

        // add final join -- joins on first field of each input
        let awvc = "SELECT article.id, article.user, title, url, vc.votes FROM article, vc WHERE \
                    article.id = vc.id;"
            .to_flow_parts(&mut inc, Some("awvc".into()))
            .unwrap();

        // add user karma
        let _karma = "SELECT awvc.user, SUM(awvc.votes) AS votes FROM awvc;"
            .to_flow_parts(&mut inc, Some("karma".into()))
            .unwrap();
    }

    // run the application
    println!("now running!");
    web::run(g).unwrap();
}

#[cfg(not(feature="web"))]
fn main() {
    unreachable!("compile with --features=web to build the web frontend");
}
