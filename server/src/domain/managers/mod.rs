//
// Copyright (c) 2022 Nathan Fiedler
//
use std::time::{Duration, SystemTimeError};

pub mod backup;
pub mod process;
pub mod restore;
pub mod state;

// Return a clear and accurate description of the duration.
pub fn pretty_print_duration(duration: Result<Duration, SystemTimeError>) -> String {
    let mut result = String::new();
    match duration {
        Ok(value) => {
            let mut seconds = value.as_secs();
            if seconds > 3600 {
                let hours = seconds / 3600;
                result.push_str(format!("{} hours ", hours).as_ref());
                seconds -= hours * 3600;
            }
            if seconds > 60 {
                let minutes = seconds / 60;
                result.push_str(format!("{} minutes ", minutes).as_ref());
                seconds -= minutes * 60;
            }
            if seconds > 0 {
                result.push_str(format!("{} seconds", seconds).as_ref());
            } else if result.is_empty() {
                // special case of a zero duration
                result.push_str("0 seconds");
            }
        }
        Err(_) => result.push_str("(error)"),
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pretty_print_duration() {
        let input = Duration::from_secs(0);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "0 seconds");

        let input = Duration::from_secs(5);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "5 seconds");

        let input = Duration::from_secs(65);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "1 minutes 5 seconds");

        let input = Duration::from_secs(4949);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "1 hours 22 minutes 29 seconds");

        let input = Duration::from_secs(7300);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "2 hours 1 minutes 40 seconds");

        let input = Duration::from_secs(10090);
        let result = pretty_print_duration(Ok(input));
        assert_eq!(result, "2 hours 48 minutes 10 seconds");
    }
}
