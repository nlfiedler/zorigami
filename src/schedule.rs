//
// Copyright (c) 2019 Nathan Fiedler
//
use chrono::prelude::*;
use juniper::GraphQLEnum;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// The day of the week, for weekly and monthly schedules.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, GraphQLEnum)]
pub enum DayOfWeek {
    Sun,
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
}

impl DayOfWeek {
    /// Return true if the given time is the same weekday as this.
    pub fn same_day(self, time: SystemTime) -> bool {
        let datetime = DateTime::<Utc>::from(time);
        let weekday = datetime.weekday();
        self.number_from_sunday() == weekday.number_from_sunday()
    }

    /// Return the number for the day of the week, starting with Sunday as 1.
    fn number_from_sunday(self) -> u32 {
        match self {
            DayOfWeek::Sun => 1,
            DayOfWeek::Mon => 2,
            DayOfWeek::Tue => 3,
            DayOfWeek::Wed => 4,
            DayOfWeek::Thu => 5,
            DayOfWeek::Fri => 6,
            DayOfWeek::Sat => 7,
        }
    }
}

impl From<u32> for DayOfWeek {
    fn from(dow: u32) -> Self {
        match dow {
            1 => DayOfWeek::Sun,
            2 => DayOfWeek::Mon,
            3 => DayOfWeek::Tue,
            4 => DayOfWeek::Wed,
            5 => DayOfWeek::Thu,
            6 => DayOfWeek::Fri,
            _ => DayOfWeek::Sat,
        }
    }
}

///
/// Represents the range in time during the day in which to run the backup. The
/// time is represented in 24-hour format, without a timezone (i.e. "naive").
/// The start and stop times can be reversed so as to span the midnight hour.
///
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TimeRange {
    /// Seconds from midnight at which to start.
    pub start: u32,
    /// Seconds from midnight at which to stop.
    pub stop: u32,
}

impl TimeRange {
    /// Construct a new range using the given hour/minute values.
    pub fn new(start_hour: u32, start_min: u32, stop_hour: u32, stop_min: u32) -> Self {
        let start_time = NaiveTime::from_hms(start_hour, start_min, 0);
        let stop_time = NaiveTime::from_hms(stop_hour, stop_min, 0);
        Self {
            start: start_time.num_seconds_from_midnight(),
            stop: stop_time.num_seconds_from_midnight(),
        }
    }

    /// Construct a new range using the given seconds-since-midnight values.
    ///
    /// Invalid values (greater than 86,400) are set to zero.
    pub fn new_secs(start_time: u32, stop_time: u32) -> Self {
        let start = if start_time < 86_400 { start_time } else { 0 };
        let stop = if stop_time < 86_400 { stop_time } else { 0 };
        Self { start, stop }
    }

    /// Return true if the given time falls within the defined range.
    pub fn within(&self, time: SystemTime) -> bool {
        let datetime = DateTime::<Utc>::from(time);
        let the_time = datetime.num_seconds_from_midnight();
        if self.stop < self.start {
            self.start <= the_time || the_time < self.stop
        } else {
            self.start <= the_time && the_time < self.stop
        }
    }

    /// Compute the time at which to stop, according to this time range.
    pub fn stop_time(&self, time: SystemTime) -> SystemTime {
        let datetime = DateTime::<Utc>::from(time);
        let the_time = datetime.num_seconds_from_midnight();
        if self.stop < the_time {
            let delta = 86_400 - (the_time - self.stop);
            time + Duration::from_secs(delta as u64)
        } else {
            let delta = self.stop - the_time;
            time + Duration::from_secs(delta as u64)
        }
    }
}

/// The day of the month, for monthly schedules.
#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum DayOfMonth {
    First(DayOfWeek),
    Second(DayOfWeek),
    Third(DayOfWeek),
    Fourth(DayOfWeek),
    Fifth(DayOfWeek),
    Day(u8),
}

impl DayOfMonth {
    /// Return true if the given time is the same day of the month as this.
    pub fn same_day(self, time: SystemTime) -> bool {
        let datetime = DateTime::<Utc>::from(time);
        let day = datetime.day();
        match self {
            DayOfMonth::Day(d) => day == d as u32,
            DayOfMonth::First(ref dow) => day < 8 && dow.same_day(time),
            DayOfMonth::Second(ref dow) => day > 7 && day < 15 && dow.same_day(time),
            DayOfMonth::Third(ref dow) => day > 14 && day < 22 && dow.same_day(time),
            DayOfMonth::Fourth(ref dow) => day > 21 && day < 29 && dow.same_day(time),
            DayOfMonth::Fifth(ref dow) => day > 28 && dow.same_day(time),
        }
    }
}

impl From<u32> for DayOfMonth {
    fn from(day: u32) -> Self {
        DayOfMonth::Day((day % 31) as u8)
    }
}

/// A schedule for when to run the backup.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Schedule {
    Hourly,
    Daily(Option<TimeRange>),
    Weekly(Option<(DayOfWeek, Option<TimeRange>)>),
    Monthly(Option<(DayOfMonth, Option<TimeRange>)>),
}

impl Schedule {
    /// Determine if enough time has elapsed such that it is now time to run the
    /// backup. This does not consider the time range, so the caller must follow
    /// up with a call to the `within_range()` function with the current time to
    /// be sure that the backup should be run now.
    pub fn past_due(&self, then: SystemTime) -> bool {
        if let Ok(delta) = then.elapsed() {
            let as_secs = delta.as_secs();
            //
            // For those schedules that do not specify a day or range, just go
            // by how many seconds it has been since the given time.
            //
            // For the other cases, if it has been long enough that it is
            // unlikely to overlap, then it is considered to be past due.
            //
            // Months are considered to be 28 days, for simplicity.
            //
            match *self {
                Schedule::Hourly => as_secs > 3600,
                Schedule::Daily(None) => as_secs > 86_400,
                Schedule::Daily(Some(_)) => as_secs > 43_200,
                Schedule::Weekly(None) => as_secs > 604_800,
                Schedule::Weekly(Some(_)) => as_secs > 302_400,
                Schedule::Monthly(None) => as_secs > 2_419_200, // 28 days
                Schedule::Monthly(Some(_)) => as_secs > 1_209_600, // 14 days
            }
        } else {
            false
        }
    }

    /// Return true if the given time falls within the range specified by this
    /// schedule, if any. The time should be the current time ("now").
    pub fn within_range(&self, time: SystemTime) -> bool {
        match *self {
            Schedule::Hourly => true,
            Schedule::Daily(None) => true,
            Schedule::Daily(Some(ref range)) => range.within(time),
            Schedule::Weekly(None) => true,
            Schedule::Weekly(Some((ref dow, None))) => dow.same_day(time),
            Schedule::Weekly(Some((ref dow, Some(ref range)))) => {
                dow.same_day(time) && range.within(time)
            }
            Schedule::Monthly(None) => true,
            Schedule::Monthly(Some((ref dom, None))) => dom.same_day(time),
            Schedule::Monthly(Some((ref dom, Some(ref range)))) => {
                dom.same_day(time) && range.within(time)
            }
        }
    }

    /// Return the time at which the backup should stop.
    ///
    /// Will return `None` if there is no stop time (i.e. no time range).
    pub fn stop_time(&self, time: SystemTime) -> Option<SystemTime> {
        match *self {
            Schedule::Hourly => None,
            Schedule::Daily(None) => None,
            Schedule::Daily(Some(ref range)) => Some(range.stop_time(time)),
            Schedule::Weekly(None) => None,
            Schedule::Weekly(Some((_, None))) => None,
            Schedule::Weekly(Some((_, Some(ref range)))) => Some(range.stop_time(time)),
            Schedule::Monthly(None) => None,
            Schedule::Monthly(Some((_, None))) => None,
            Schedule::Monthly(Some((_, Some(ref range)))) => Some(range.stop_time(time)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_time_range() {
        let test_data = vec![
            // normal time range checking
            (10, 5, 12, 30, 10, 5, true),
            (10, 5, 12, 30, 10, 15, true),
            (10, 5, 12, 30, 11, 0, true),
            (10, 5, 12, 30, 12, 15, true),
            (10, 5, 12, 30, 12, 30, false),
            (10, 5, 12, 30, 10, 0, false),
            (10, 5, 12, 30, 12, 35, false),
            (10, 5, 12, 30, 9, 0, false),
            (10, 5, 12, 30, 13, 0, false),
            // backward time range (stop before start)
            (22, 0, 6, 0, 22, 0, true),
            (22, 0, 6, 0, 3, 30, true),
            (22, 0, 6, 0, 5, 59, true),
            (22, 0, 6, 0, 23, 0, true),
            (22, 0, 6, 0, 6, 0, false),
            (22, 0, 6, 0, 6, 1, false),
            (22, 0, 6, 0, 21, 59, false),
            (22, 0, 6, 0, 21, 0, false),
            // midnight edge case
            (17, 0, 0, 0, 17, 0, true),
            (17, 0, 0, 0, 18, 0, true),
            (17, 0, 0, 0, 23, 59, true),
            (17, 0, 0, 0, 0, 0, false),
            (17, 0, 0, 0, 0, 30, false),
            (17, 0, 0, 0, 12, 0, false),
            (17, 0, 0, 0, 16, 59, false),
        ];
        for (idx, values) in test_data.iter().enumerate() {
            let range = TimeRange::new(values.0, values.1, values.2, values.3);
            let then: SystemTime = Utc.ymd(2003, 8, 30).and_hms(values.4, values.5, 0).into();
            assert_eq!(range.within(then), values.6, "index: {}", idx);
        }
    }

    #[test]
    fn test_hourly() {
        let sched = Schedule::Hourly;
        let duration = Duration::new(3700, 0);
        let then = SystemTime::now() - duration;
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        let then = SystemTime::now() + duration;
        assert!(!sched.past_due(then));
        assert!(sched.within_range(then));
    }

    #[test]
    fn test_daily() {
        // overdue with no time range
        let sched = Schedule::Daily(None);
        let duration = Duration::new(90_000, 0);
        let then = SystemTime::now() - duration;
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // overdue but not within the given range
        let range = TimeRange::new(12, 0, 18, 0);
        let sched = Schedule::Daily(Some(range));
        let then: SystemTime = Utc.ymd(2018, 10, 14).and_hms(9, 10, 11).into();
        assert!(sched.past_due(then));
        assert!(!sched.within_range(then));

        // overdue and within the given range
        let then: SystemTime = Utc.ymd(2018, 4, 26).and_hms(14, 10, 11).into();
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // overdue but not within the given range
        let then: SystemTime = Utc.ymd(2018, 4, 26).and_hms(20, 10, 11).into();
        assert!(sched.past_due(then));
        assert!(!sched.within_range(then));
    }

    #[test]
    fn test_daily_fuzzy() {
        // Specify a time that was not exactly 24 hours ago, but close enough
        // and within the given time range, so it should run now.
        let now = SystemTime::now();
        let then = now - Duration::new(82_800, 0);
        let now = DateTime::<Utc>::from(now);
        // "subtract" two hours by adding 22 and taking the modulus
        let range = TimeRange::new((now.hour() + 22) % 24, 0, (now.hour() + 2) % 24, 0);
        let sched = Schedule::Daily(Some(range));
        assert!(sched.past_due(then));
        assert!(sched.within_range(now.into()));
    }

    #[test]
    fn test_weekly() {
        // overdue with no day of week
        let sched = Schedule::Weekly(None);
        let then: SystemTime = Utc.ymd(2018, 5, 8).and_hms(9, 10, 11).into();
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // right day of week, no time range
        let sched = Schedule::Weekly(Some((DayOfWeek::Tue, None)));
        let then: SystemTime = Utc.ymd(2018, 5, 8).and_hms(14, 10, 11).into();
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // wrong day of the week
        let range = TimeRange::new(12, 0, 18, 0);
        let sched = Schedule::Weekly(Some((DayOfWeek::Thu, Some(range))));
        assert!(sched.past_due(then));
        assert!(!sched.within_range(then));

        // right day of the week, wrong time
        let range = TimeRange::new(10, 0, 12, 0);
        let sched = Schedule::Weekly(Some((DayOfWeek::Tue, Some(range))));
        assert!(sched.past_due(then));
        assert!(!sched.within_range(then));

        // right day of the week, within time range
        let range = TimeRange::new(12, 0, 18, 0);
        let sched = Schedule::Weekly(Some((DayOfWeek::Tue, Some(range))));
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));
    }

    #[test]
    fn test_weekly_fuzzy() {
        // Specify a time that was not exactly 7 days ago, but close enough
        // and within the given time range, so it should run now.
        let now = SystemTime::now();
        let then = now - Duration::new(601_200, 0);
        let now = DateTime::<Utc>::from(now);
        // "subtract" two hours by adding 22 and taking the modulus
        let range = TimeRange::new((now.hour() + 22) % 24, 0, (now.hour() + 2) % 24, 0);
        let dow = DayOfWeek::from(now.weekday().number_from_sunday());
        let sched = Schedule::Weekly(Some((dow, Some(range))));
        assert!(sched.past_due(then));
        assert!(sched.within_range(now.into()));
    }

    #[test]
    fn test_monthly_day() {
        // monthly with no day or time range
        let sched = Schedule::Monthly(None);
        let then: SystemTime = Utc.ymd(2018, 5, 8).and_hms(14, 10, 11).into();
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // with a specific date, but too early
        let sched = Schedule::Monthly(Some((DayOfMonth::Day(7), None)));
        assert!(sched.past_due(then));
        assert!(!sched.within_range(then));

        // with a specific date, too late
        let sched = Schedule::Monthly(Some((DayOfMonth::Day(9), None)));
        assert!(sched.past_due(then));
        assert!(!sched.within_range(then));

        // with a specific date, right on time
        let sched = Schedule::Monthly(Some((DayOfMonth::Day(8), None)));
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // with a specific date and time range
        let range = TimeRange::new(12, 0, 18, 0);
        let sched = Schedule::Monthly(Some((DayOfMonth::Day(8), Some(range))));
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // with a specific date but not within time range
        let range = TimeRange::new(10, 0, 12, 0);
        let sched = Schedule::Monthly(Some((DayOfMonth::Day(8), Some(range))));
        assert!(sched.past_due(then));
        assert!(!sched.within_range(then));
    }

    #[test]
    fn test_monthly_weeks() {
        // 2018-05-03 is the first Thursday of the month
        let date1: SystemTime = Utc.ymd(2018, 5, 3).and_hms(14, 10, 11).into();
        // 2018-05-08 is the second Tuesday of the month
        let date2: SystemTime = Utc.ymd(2018, 5, 8).and_hms(14, 10, 11).into();
        // 2018-05-20 is the third Sunday of the month
        let date3: SystemTime = Utc.ymd(2018, 5, 20).and_hms(14, 10, 11).into();
        // 2018-05-26 is the fourth Saturday of the month
        let date4: SystemTime = Utc.ymd(2018, 5, 26).and_hms(14, 10, 11).into();
        // 2018-05-30 is the fifth Wednesday of the month
        let date5: SystemTime = Utc.ymd(2018, 5, 30).and_hms(14, 10, 11).into();
        let test_data = vec![
            // first week
            (date1, DayOfMonth::First(DayOfWeek::Thu), true),
            (date1, DayOfMonth::Second(DayOfWeek::Thu), false),
            (date1, DayOfMonth::Third(DayOfWeek::Thu), false),
            (date1, DayOfMonth::Fourth(DayOfWeek::Thu), false),
            (date1, DayOfMonth::Fifth(DayOfWeek::Thu), false),
            // second week
            (date2, DayOfMonth::First(DayOfWeek::Tue), false),
            (date2, DayOfMonth::Second(DayOfWeek::Tue), true),
            (date2, DayOfMonth::Third(DayOfWeek::Tue), false),
            (date2, DayOfMonth::Fourth(DayOfWeek::Tue), false),
            (date2, DayOfMonth::Fifth(DayOfWeek::Tue), false),
            // third week
            (date3, DayOfMonth::First(DayOfWeek::Sun), false),
            (date3, DayOfMonth::Second(DayOfWeek::Sun), false),
            (date3, DayOfMonth::Third(DayOfWeek::Sun), true),
            (date3, DayOfMonth::Fourth(DayOfWeek::Sun), false),
            (date3, DayOfMonth::Fifth(DayOfWeek::Sun), false),
            // fourth week
            (date4, DayOfMonth::First(DayOfWeek::Sat), false),
            (date4, DayOfMonth::Second(DayOfWeek::Sat), false),
            (date4, DayOfMonth::Third(DayOfWeek::Sat), false),
            (date4, DayOfMonth::Fourth(DayOfWeek::Sat), true),
            (date4, DayOfMonth::Fifth(DayOfWeek::Sat), false),
            // fifth week
            (date5, DayOfMonth::First(DayOfWeek::Wed), false),
            (date5, DayOfMonth::Second(DayOfWeek::Wed), false),
            (date5, DayOfMonth::Third(DayOfWeek::Wed), false),
            (date5, DayOfMonth::Fourth(DayOfWeek::Wed), false),
            (date5, DayOfMonth::Fifth(DayOfWeek::Wed), true),
        ];
        for (idx, values) in test_data.iter().enumerate() {
            let sched = Schedule::Monthly(Some((values.1, None)));
            assert!(sched.past_due(values.0));
            assert_eq!(sched.within_range(values.0), values.2, "index: {}", idx);
        }
    }

    #[test]
    fn test_monthly_weeks_range() {
        // 2018-05-31 is the fifth Thursday of the month
        let then: SystemTime = Utc.ymd(2018, 5, 31).and_hms(14, 10, 11).into();
        let range = TimeRange::new(12, 0, 18, 0);
        let sched = Schedule::Monthly(Some((DayOfMonth::Fifth(DayOfWeek::Thu), Some(range))));
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // not within time range
        let range = TimeRange::new(10, 0, 12, 0);
        let sched = Schedule::Monthly(Some((DayOfMonth::Fifth(DayOfWeek::Thu), Some(range))));
        assert!(sched.past_due(then));
        assert!(!sched.within_range(then));
    }

    #[test]
    fn test_monthly_fuzzy() {
        // Specify a time that was not exactly 28 days ago, but close enough
        // and within the given time range, so it should run now.
        let now = SystemTime::now();
        let then = now - Duration::new(2_415_600, 0);
        let now = DateTime::<Utc>::from(now);
        // "subtract" two hours by adding 22 and taking the modulus
        let range = TimeRange::new((now.hour() + 22) % 24, 0, (now.hour() + 2) % 24, 0);
        let dom = DayOfMonth::from(now.day());
        let sched = Schedule::Monthly(Some((dom, Some(range))));
        assert!(sched.past_due(then));
        assert!(sched.within_range(now.into()));
    }

    #[test]
    fn test_stop_time() {
        let then: SystemTime = Utc.ymd(2018, 5, 31).and_hms(14, 10, 11).into();

        // schedules without a range have no stop time
        let sched = Schedule::Hourly;
        assert!(sched.stop_time(then).is_none());
        let sched = Schedule::Daily(None);
        assert!(sched.stop_time(then).is_none());
        let sched = Schedule::Weekly(None);
        assert!(sched.stop_time(then).is_none());
        let sched = Schedule::Weekly(Some((DayOfWeek::Thu, None)));
        assert!(sched.stop_time(then).is_none());
        let sched = Schedule::Monthly(None);
        assert!(sched.stop_time(then).is_none());
        let sched = Schedule::Monthly(Some((DayOfMonth::Fifth(DayOfWeek::Thu), None)));
        assert!(sched.stop_time(then).is_none());

        // daily
        let range = TimeRange::new(12, 0, 18, 30);
        let sched = Schedule::Daily(Some(range));
        let stop_time = sched.stop_time(then).unwrap();
        let stop_time = DateTime::<Utc>::from(stop_time);
        assert_eq!(stop_time.hour(), 18);
        assert_eq!(stop_time.minute(), 30);

        // weekly
        let range = TimeRange::new(12, 0, 18, 30);
        let sched = Schedule::Weekly(Some((DayOfWeek::Thu, Some(range))));
        let stop_time = sched.stop_time(then).unwrap();
        let stop_time = DateTime::<Utc>::from(stop_time);
        assert_eq!(stop_time.hour(), 18);
        assert_eq!(stop_time.minute(), 30);

        // monthly
        let range = TimeRange::new(12, 0, 18, 30);
        let sched = Schedule::Monthly(Some((DayOfMonth::Fifth(DayOfWeek::Thu), Some(range))));
        let stop_time = sched.stop_time(then).unwrap();
        let stop_time = DateTime::<Utc>::from(stop_time);
        assert_eq!(stop_time.hour(), 18);
        assert_eq!(stop_time.minute(), 30);
    }

    #[test]
    fn test_stop_time_reverse() {
        let then: SystemTime = Utc.ymd(2018, 5, 31).and_hms(21, 10, 11).into();
        let range = TimeRange::new(20, 0, 4, 0);
        let sched = Schedule::Daily(Some(range));
        let stop_time = sched.stop_time(then).unwrap();
        let stop_time = DateTime::<Utc>::from(stop_time);
        assert_eq!(stop_time.year(), 2018);
        assert_eq!(stop_time.month(), 6);
        assert_eq!(stop_time.day(), 1);
        assert_eq!(stop_time.hour(), 4);
        assert_eq!(stop_time.minute(), 0);
    }
}
