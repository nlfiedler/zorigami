//
// Copyright (c) 2020 Nathan Fiedler
//
use chrono::prelude::*;

/// The day of the week, for weekly and monthly schedules.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
    pub fn same_day(self, datetime: DateTime<Utc>) -> bool {
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
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TimeRange {
    /// Seconds from midnight at which to start.
    pub start: u32,
    /// Seconds from midnight at which to stop.
    pub stop: u32,
}

impl TimeRange {
    /// Construct a new range using the given hour/minute values.
    pub fn new(start_hour: u32, start_min: u32, stop_hour: u32, stop_min: u32) -> Self {
        let start_time = NaiveTime::from_hms_opt(start_hour, start_min, 0)
            .unwrap_or_else(|| NaiveTime::default());
        let stop_time =
            NaiveTime::from_hms_opt(stop_hour, stop_min, 0).unwrap_or_else(|| NaiveTime::default());
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
    pub fn within(&self, datetime: DateTime<Utc>) -> bool {
        let the_time = datetime.num_seconds_from_midnight();
        if self.stop < self.start {
            self.start <= the_time || the_time < self.stop
        } else {
            self.start <= the_time && the_time < self.stop
        }
    }

    /// Compute the time at which to stop, according to this time range.
    pub fn stop_time(&self, datetime: DateTime<Utc>) -> DateTime<Utc> {
        let the_time = datetime.num_seconds_from_midnight();
        if self.stop < the_time {
            let delta = 86_400 - (the_time - self.stop);
            datetime + chrono::Duration::seconds(delta as i64)
        } else {
            let delta = self.stop - the_time;
            datetime + chrono::Duration::seconds(delta as i64)
        }
    }
}

/// The day of the month, for monthly schedules.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
    pub fn same_day(self, datetime: DateTime<Utc>) -> bool {
        let day = datetime.day();
        match self {
            DayOfMonth::Day(d) => day == d as u32,
            DayOfMonth::First(ref dow) => day < 8 && dow.same_day(datetime),
            DayOfMonth::Second(ref dow) => day > 7 && day < 15 && dow.same_day(datetime),
            DayOfMonth::Third(ref dow) => day > 14 && day < 22 && dow.same_day(datetime),
            DayOfMonth::Fourth(ref dow) => day > 21 && day < 29 && dow.same_day(datetime),
            DayOfMonth::Fifth(ref dow) => day > 28 && dow.same_day(datetime),
        }
    }
}

impl From<u32> for DayOfMonth {
    fn from(day: u32) -> Self {
        DayOfMonth::Day((day % 31) as u8)
    }
}

/// A schedule for when to run the backup.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
    pub fn past_due(&self, then: DateTime<Utc>) -> bool {
        let elapsed = Utc::now() - then;
        let as_secs = elapsed.num_seconds();
        if as_secs >= 0 {
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
    pub fn within_range(&self, time: DateTime<Utc>) -> bool {
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

    /// Return true if the given time is past due and the current time falls
    /// within the specified range, if any.
    pub fn is_ready(&self, then: DateTime<Utc>) -> bool {
        self.past_due(then) && self.within_range(Utc::now())
    }

    /// Return the time at which the backup should stop.
    ///
    /// Will return `None` if there is no stop time (i.e. no time range).
    ///
    /// The time should be the current time ("now").
    pub fn stop_time(&self, time: DateTime<Utc>) -> Option<DateTime<Utc>> {
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
    use chrono::Duration;

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
            // edge cases
            (13, 0, 12, 59, 13, 1, true),
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
            let then = Utc
                .with_ymd_and_hms(2003, 8, 30, values.4, values.5, 0)
                .unwrap();
            assert_eq!(range.within(then), values.6, "index: {}", idx);
        }
    }

    #[test]
    fn test_hourly() {
        let sched = Schedule::Hourly;
        let duration = Duration::seconds(3700);
        let then = Utc::now() - duration;
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        let then = Utc::now() + duration;
        assert!(!sched.past_due(then));
        assert!(sched.within_range(then));
    }

    #[test]
    fn test_daily() {
        // overdue with no time range
        let sched = Schedule::Daily(None);
        let duration = Duration::hours(25);
        let then = Utc::now() - duration;
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // overdue but not within the given range
        let range = TimeRange::new(12, 0, 18, 0);
        let sched = Schedule::Daily(Some(range));
        let then = Utc.with_ymd_and_hms(2018, 10, 14, 9, 10, 11).unwrap();
        assert!(sched.past_due(then));
        assert!(!sched.within_range(then));

        // overdue and within the given range
        let then = Utc.with_ymd_and_hms(2018, 4, 26, 14, 10, 11).unwrap();
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // overdue but not within the given range
        let then = Utc.with_ymd_and_hms(2018, 4, 26, 20, 10, 11).unwrap();
        assert!(sched.past_due(then));
        assert!(!sched.within_range(then));
    }

    #[test]
    fn test_daily_fuzzy() {
        // Specify a time that was not exactly 24 hours ago, but close enough
        // and within the given time range, so it should run now.
        let now = Utc::now();
        let then = now - Duration::hours(23);
        // "subtract" two hours by adding 22 and taking the modulus
        let range = TimeRange::new((now.hour() + 22) % 24, 0, (now.hour() + 2) % 24, 0);
        let sched = Schedule::Daily(Some(range));
        assert!(sched.past_due(then));
        assert!(sched.within_range(now));
    }

    #[test]
    fn test_weekly() {
        // overdue with no day of week
        let sched = Schedule::Weekly(None);
        let then = Utc.with_ymd_and_hms(2018, 5, 8, 9, 10, 11).unwrap();
        assert!(sched.past_due(then));
        assert!(sched.within_range(then));

        // right day of week, no time range
        let sched = Schedule::Weekly(Some((DayOfWeek::Tue, None)));
        let then = Utc.with_ymd_and_hms(2018, 5, 8, 14, 10, 11).unwrap();
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
        let now = Utc::now();
        let then = now - Duration::hours(167);
        // "subtract" two hours by adding 22 and taking the modulus
        let range = TimeRange::new((now.hour() + 22) % 24, 0, (now.hour() + 2) % 24, 0);
        let dow = DayOfWeek::from(now.weekday().number_from_sunday());
        let sched = Schedule::Weekly(Some((dow, Some(range))));
        assert!(sched.past_due(then));
        assert!(sched.within_range(now));
    }

    #[test]
    fn test_monthly_day() {
        // monthly with no day or time range
        let sched = Schedule::Monthly(None);
        let then = Utc.with_ymd_and_hms(2018, 5, 8, 14, 10, 11).unwrap();
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
        let date1 = Utc.with_ymd_and_hms(2018, 5, 3, 14, 10, 11).unwrap();
        // 2018-05-08 is the second Tuesday of the month
        let date2 = Utc.with_ymd_and_hms(2018, 5, 8, 14, 10, 11).unwrap();
        // 2018-05-20 is the third Sunday of the month
        let date3 = Utc.with_ymd_and_hms(2018, 5, 20, 14, 10, 11).unwrap();
        // 2018-05-26 is the fourth Saturday of the month
        let date4 = Utc.with_ymd_and_hms(2018, 5, 26, 14, 10, 11).unwrap();
        // 2018-05-30 is the fifth Wednesday of the month
        let date5 = Utc.with_ymd_and_hms(2018, 5, 30, 14, 10, 11).unwrap();
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
        let then = Utc.with_ymd_and_hms(2018, 5, 31, 14, 10, 11).unwrap();
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
        let now = Utc::now();
        let then = now - Duration::hours(671);
        // "subtract" two hours by adding 22 and taking the modulus
        let range = TimeRange::new((now.hour() + 22) % 24, 0, (now.hour() + 2) % 24, 0);
        let dom = DayOfMonth::from(now.day());
        let sched = Schedule::Monthly(Some((dom, Some(range))));
        assert!(sched.past_due(then));
        assert!(sched.within_range(now));
    }

    #[test]
    fn test_stop_time() {
        let then = Utc.with_ymd_and_hms(2018, 5, 31, 14, 10, 11).unwrap();

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
        assert_eq!(stop_time.hour(), 18);
        assert_eq!(stop_time.minute(), 30);

        // weekly
        let range = TimeRange::new(12, 0, 18, 30);
        let sched = Schedule::Weekly(Some((DayOfWeek::Thu, Some(range))));
        let stop_time = sched.stop_time(then).unwrap();
        assert_eq!(stop_time.hour(), 18);
        assert_eq!(stop_time.minute(), 30);

        // monthly
        let range = TimeRange::new(12, 0, 18, 30);
        let sched = Schedule::Monthly(Some((DayOfMonth::Fifth(DayOfWeek::Thu), Some(range))));
        let stop_time = sched.stop_time(then).unwrap();
        assert_eq!(stop_time.hour(), 18);
        assert_eq!(stop_time.minute(), 30);
    }

    #[test]
    fn test_stop_time_reverse() {
        let then = Utc.with_ymd_and_hms(2018, 5, 31, 21, 10, 11).unwrap();
        let range = TimeRange::new(20, 0, 4, 0);
        let sched = Schedule::Daily(Some(range));
        let stop_time = sched.stop_time(then).unwrap();
        assert_eq!(stop_time.year(), 2018);
        assert_eq!(stop_time.month(), 6);
        assert_eq!(stop_time.day(), 1);
        assert_eq!(stop_time.hour(), 4);
        assert_eq!(stop_time.minute(), 0);
    }

    #[test]
    fn test_is_ready_hourly() {
        let schedule = Schedule::Hourly;
        let hour_ago = chrono::Duration::hours(2);
        let end_time = Utc::now() - hour_ago;
        assert!(schedule.is_ready(end_time));
        let end_time = Utc::now();
        assert!(!schedule.is_ready(end_time));
    }

    #[test]
    fn test_is_ready_daily() {
        let schedule = Schedule::Daily(None);
        let day_ago = chrono::Duration::hours(25);
        let end_time = Utc::now() - day_ago;
        assert!(schedule.is_ready(end_time));
        let end_time = Utc::now();
        assert!(!schedule.is_ready(end_time));
    }
}
