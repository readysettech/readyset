use std::collections::HashMap;
use std::fmt;
use std::ops::Range;

use anyhow::{anyhow, Error};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Interval {
    pub start: u64,
    pub end: u64,
}

impl Interval {
    fn contains(&self, value: u64) -> bool {
        value >= self.start && value <= self.end
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.start, self.end)
    }
}

impl From<Range<u64>> for Interval {
    fn from(range: Range<u64>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Mysql56Gtid {
    // Server is the SID of the server that originally committed the transaction.
    pub server: Uuid,

    // Sequence is the sequence number of the transaction within a given Server's scope.
    pub sequence: u64,
}

impl TryFrom<&str> for Mysql56Gtid {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Error> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow!(
                "invalid MySQL 5.6 GTID: {}. Expecting UUID:Sequence",
                s
            ));
        }

        let server = Uuid::try_parse(parts[0])
            .map_err(|_| anyhow!("invalid MySQL 5.6 GTID Server ID: {}. Expecting a UUID", s))?;

        let sequence = parts[1]
            .parse()
            .map_err(|_| anyhow!("invalid MySQL 5.6 GTID Sequence number : {}", s))?;

        Ok(Self { server, sequence })
    }
}

impl fmt::Display for Mysql56Gtid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.server, self.sequence)
    }
}

pub fn parse_interval(s: &str) -> Result<Interval, anyhow::Error> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() < 1 || parts.len() > 2 {
        return Err(anyhow!("invalid interval: {}", s));
    }

    let start = parts[0].parse()?;
    if start < 1 {
        return Err(anyhow!("invalid interval: {}. Start must be > 0", s));
    }
    let end = parts.get(1).map(|s| s.parse()).unwrap_or(Ok(start))?;
    Ok((start..end).into())
}

pub fn contains_interval(intervals: &[Interval], interval: &Interval) -> bool {
    for i in intervals {
        if i.contains(interval.start) && i.contains(interval.end) {
            return true;
        }
    }
    false
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
pub struct Mysql56GtidSet {
    pub intervals: HashMap<Uuid, Vec<Interval>>,
}

impl TryFrom<&str> for Mysql56GtidSet {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Error> {
        let mut sid_to_intervals = HashMap::<Uuid, Vec<Interval>>::new();

        if s.is_empty() {
            return Ok(Self {
                intervals: sid_to_intervals,
            });
        }

        // gtid_set: uuid_set [, uuid_set] ...
        for part in s.split(',') {
            // uuid_set: uuid:interval[:interval]...
            let parts: Vec<&str> = part.trim().split(':').collect();
            if parts.len() < 2 {
                return Err(anyhow!("invalid GTID set: {}", s));
            }

            let sid = Uuid::try_parse(parts[0])?;
            let mut intervals = Vec::new();
            for interval_str in parts[1..].iter() {
                let interval = parse_interval(interval_str)?;

                // According to MySQL 5.6 code:
                //   "The end of an interval may be 0, but any interval that has an
                //    endpoint that is smaller than the start is discarded."
                if interval.start > interval.end {
                    continue;
                }
                intervals.push(interval);
            }

            // We might have discarded all the intervals.
            if intervals.is_empty() {
                continue;
            }

            intervals.sort_by_key(|interval| interval.start);

            if let Some(existing_intervals) = sid_to_intervals.get_mut(&sid) {
                existing_intervals.extend(intervals);
                existing_intervals.sort_by_key(|interval| interval.start);
            } else {
                sid_to_intervals.insert(sid, intervals);
            }
        }

        Ok(Self {
            intervals: sid_to_intervals,
        })
    }
}

impl fmt::Display for Mysql56GtidSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sid_to_intervals = self.sorted_intervals();

        for (pos, (sid, intervals)) in sid_to_intervals.iter().enumerate() {
            if pos > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", sid)?;

            for interval in *intervals {
                write!(f, ":{}", interval.start)?;
                if interval.end != interval.start {
                    write!(f, "-{}", interval.end)?;
                }
            }
        }

        Ok(())
    }
}

impl Mysql56GtidSet {
    fn sorted_intervals(&self) -> Vec<(&Uuid, &Vec<Interval>)> {
        let mut sid_to_intervals: Vec<(&Uuid, &Vec<Interval>)> = self.intervals.iter().collect();
        sid_to_intervals.sort_by_key(|(sid, _)| *sid);
        sid_to_intervals
    }

    // Returns the last gtid as string
    // For gtidset having multiple SIDs or multiple intervals
    // it just returns the last SID with last interval
    pub fn last(&self) -> String {
        let sid_to_intervals = self.sorted_intervals();
        if sid_to_intervals.is_empty() {
            return "".to_string();
        }
        let (sid, intervals) = sid_to_intervals.last().unwrap();
        let mut result = sid.to_string();

        if let Some(interval) = intervals.last() {
            result.push_str(&format!(":{}", interval.end));
        }
        result
    }

    pub fn contains_gtid(&self, gtid: &Mysql56Gtid) -> bool {
        if let Some(intervals) = self.intervals.get(&gtid.server) {
            for interval in intervals {
                if interval.contains(gtid.sequence) {
                    return true;
                }
            }
        }
        false
    }

    pub fn contains(&self, other: &Mysql56GtidSet) -> bool {
        // Check each SID in the other set.
        for (sid, other_intervals) in other.intervals.iter() {
            // Get our intervals for this SID.
            if let Some(self_intervals) = self.intervals.get(sid) {
                // Make sure the other set's interval is contained in one of our intervals.
                for other_interval in other_intervals {
                    if contains_interval(self_intervals, other_interval) {
                        continue;
                    }
                    // We don't have an interval that contains the other set's interval.
                    return false;
                }
            } else {
                // We don't have any intervals for this SID.
                return false;
            }
        }
        // Our intervals contain all of the other set's intervals.
        true
    }
}

impl PartialEq for Mysql56GtidSet {
    fn eq(&self, other: &Self) -> bool {
        self.intervals == other.intervals
    }
}

impl PartialOrd for Mysql56GtidSet {
    // Checks if a given position identified by GTID set happened before or after another position
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self == other {
            return Some(std::cmp::Ordering::Equal);
        }

        // Check if we have the same number of SIDs
        // FIXME: Figure out how to handle cases when the numbers of SIDs are different
        if self.intervals.keys().len() != other.intervals.keys().len() {
            return None;
        }

        // Check each SID in the other set
        // There are multiple options for its ordering:
        // - We do not have the SID in our set. FIXME: We currently do not know how to handle this.
        // - We have the SID in our set, meaning we need to compare intervals:
        //    - If the last interval in the other set ends before the last interval in our set, then
        //      we are ahead of the other set.
        //    - If the last interval in the other set ends after the last interval in our set, then
        //      we are behind the other set.
        //    - If the last interval in the other set ends at the same time as the last interval in
        //      our set, the intervals are the same, so we need to compare other SIDs.
        //
        for (other_sid, other_intervals) in other.intervals.iter() {
            // Get our intervals for this SID.
            if let Some(self_intervals) = self.intervals.get(other_sid) {
                // Compare the last intervals.
                let self_last_interval = self_intervals.last().unwrap();
                let other_last_interval = other_intervals.last().unwrap();

                if self_last_interval.end < other_last_interval.end {
                    // We are behind the other set.
                    return Some(std::cmp::Ordering::Less);
                } else if self_last_interval.end > other_last_interval.end {
                    // We are ahead of the other set.
                    return Some(std::cmp::Ordering::Greater);
                } else {
                    // The last intervals are the same, so we need to compare other SIDs.
                    continue;
                }
            } else {
                // We don't have any intervals for this SID
                // FIXME: Figure out what this means
                return None;
            }
        }

        // Our intervals are the same as the other set's intervals.
        Some(std::cmp::Ordering::Equal)
    }
}
