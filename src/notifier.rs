/*
 * This software is in the public domain under CC0 1.0 Universal plus a
 * Grant of Patent License.
 *
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */

//! Persistence failure tracking per request, as the InboundErrorNotifier of the Camel gateway did: the failure is
//! notified once, when it has lasted the threshold; the recovery is notified only after a failure notification.
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// What to tell the user after a store result.
#[derive(Debug, Eq, PartialEq)]
pub enum Notice {
    /// The request has failed to store for this long, without a store succeeding.
    Failing(Duration),
    /// The request stores again after failing for this long.
    Recovered(Duration),
}
struct ErrorState {
    first: Instant,
    notified: bool,
}
pub struct InboundErrors {
    threshold: Duration,
    states: HashMap<String, ErrorState>,
}
impl InboundErrors {
    pub fn new(threshold: Duration) -> Self {
        Self {
            threshold,
            states: HashMap::new(),
        }
    }
    /// A store of `request` failed at `now`. The first failure starts the tracking; a notice comes once, when the
    /// failures have lasted the threshold.
    pub fn record_error(&mut self, request: &str, now: Instant) -> Option<Notice> {
        let state = self.states.entry(request.to_string()).or_insert(ErrorState {
            first: now,
            notified: false,
        });
        let elapsed = now.saturating_duration_since(state.first);
        if !state.notified && elapsed >= self.threshold {
            state.notified = true;
            return Some(Notice::Failing(elapsed));
        }
        None
    }
    /// A store of `request` succeeded at `now`: the tracking ends, with a notice if a failure was notified.
    pub fn clear(&mut self, request: &str, now: Instant) -> Option<Notice> {
        let state = self.states.remove(request)?;
        state
            .notified
            .then(|| Notice::Recovered(now.saturating_duration_since(state.first)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const MINUTE: Duration = Duration::from_secs(60);

    #[test]
    fn error_is_notified_once_after_the_threshold() {
        let mut errors = InboundErrors::new(MINUTE);
        let start = Instant::now();
        assert_eq!(errors.record_error("r", start), None);
        assert_eq!(errors.record_error("r", start + Duration::from_secs(30)), None);
        assert_eq!(errors.record_error("r", start + MINUTE), Some(Notice::Failing(MINUTE)));
        assert_eq!(errors.record_error("r", start + MINUTE * 2), None);
    }
    #[test]
    fn a_zero_threshold_notifies_the_first_error() {
        let mut errors = InboundErrors::new(Duration::ZERO);
        let start = Instant::now();
        assert_eq!(errors.record_error("r", start), Some(Notice::Failing(Duration::ZERO)));
    }
    #[test]
    fn recovery_is_notified_only_after_an_error_notice() {
        let mut errors = InboundErrors::new(MINUTE);
        let start = Instant::now();
        errors.record_error("quiet", start);
        assert_eq!(errors.clear("quiet", start + Duration::from_secs(10)), None);
        errors.record_error("loud", start);
        errors.record_error("loud", start + MINUTE);
        assert_eq!(
            errors.clear("loud", start + MINUTE * 2),
            Some(Notice::Recovered(MINUTE * 2))
        );
    }
    #[test]
    fn clearing_a_request_without_errors_does_nothing() {
        let mut errors = InboundErrors::new(MINUTE);
        assert_eq!(errors.clear("clean", Instant::now()), None);
    }
    #[test]
    fn requests_are_tracked_separately() {
        let mut errors = InboundErrors::new(Duration::ZERO);
        let now = Instant::now();
        assert!(errors.record_error("a", now).is_some());
        assert!(errors.record_error("b", now).is_some());
        assert_eq!(errors.clear("a", now), Some(Notice::Recovered(Duration::ZERO)));
        assert_eq!(errors.record_error("b", now), None);
    }
}
