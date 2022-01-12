use hdrhistogram::serialization::interval_log;
use hdrhistogram::Histogram;
use std::time::Duration;

#[derive(Default, Clone)]
pub struct Timeline {
    // these are logarithmically spaced
    // the first histogram is 0-1s after start, the second 1-2s after start, then 2-4s, etc.
    histograms: Vec<Histograms>,
    total_duration: Duration,
}

#[derive(Clone)]
pub struct Histograms {
    processing: Histogram<u64>,
    sojourn: Histogram<u64>,
}

impl Default for Histograms {
    fn default() -> Self {
        Self {
            processing: Histogram::new_with_bounds(1, 60_000_000, 3).unwrap(),
            sojourn: Histogram::new_with_bounds(1, 60_000_000, 3).unwrap(),
        }
    }
}

impl Histograms {
    pub fn processing(&mut self, time: u64) {
        self.processing.saturating_record(time);
    }

    pub fn sojourn(&mut self, time: u64) {
        self.sojourn.saturating_record(time);
    }

    pub fn merge(&mut self, other: &Self) {
        self.processing.add(&other.processing).expect("same bounds");
        self.sojourn.add(&other.sojourn).expect("same bounds");
    }
}

impl Timeline {
    pub fn set_total_duration(&mut self, total: Duration) {
        self.total_duration = total;
    }

    pub fn histogram_for(&mut self, issued_at: Duration) -> &mut Histograms {
        let hist = ((issued_at.as_secs_f64() + 0.000000000001).ceil() as usize)
            .next_power_of_two()
            .trailing_zeros() as usize;

        if hist >= self.histograms.len() {
            self.histograms.resize(hist + 1, Histograms::default());
        }
        self.histograms.get_mut(hist).unwrap()
    }

    pub fn merge(&mut self, other: &Self) {
        for (ti, other_hs) in other.histograms.iter().enumerate() {
            if let Some(self_hs) = self.histograms.get_mut(ti) {
                self_hs.merge(other_hs);
            } else {
                self.histograms.push(other_hs.clone());
            }
        }
    }

    pub fn write<W: std::io::Write, S: hdrhistogram::serialization::Serializer>(
        &self,
        w: &mut interval_log::IntervalLogWriter<W, S>,
    ) -> Result<(), interval_log::IntervalLogWriterError<S::SerializeError>> {
        let proc_tag = interval_log::Tag::new("processing").unwrap();
        let sjrn_tag = interval_log::Tag::new("sojourn").unwrap();
        for (i, hs) in self.histograms.iter().enumerate() {
            let start = Duration::from_secs((1 << i) >> 1);
            let mut dur = Duration::from_secs(1 << i) - start;
            if self.total_duration != Duration::new(0, 0) && start + dur > self.total_duration {
                dur = self.total_duration - start;
            }
            w.write_histogram(&hs.processing, start, dur, Some(proc_tag))?;
            w.write_histogram(&hs.sojourn, start, dur, Some(sjrn_tag))?;
        }
        Ok(())
    }

    pub fn last(&self) -> Option<(&Histogram<u64>, &Histogram<u64>)> {
        self.histograms.last().map(|h| (&h.processing, &h.sojourn))
    }

    pub fn collapse(&self) -> (Histogram<u64>, Histogram<u64>) {
        let mut hists = self.histograms.iter();
        if let Some(hs) = hists.next() {
            let mut proc = hs.processing.clone();
            let mut sjrn = hs.sojourn.clone();
            for hs in hists {
                proc.add(&hs.processing).expect("same bounds");
                sjrn.add(&hs.sojourn).expect("same bounds");
            }
            (proc, sjrn)
        } else {
            (Histogram::new(1).unwrap(), Histogram::new(1).unwrap())
        }
    }
}
