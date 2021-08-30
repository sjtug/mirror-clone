use std::fmt;
use std::fmt::{Display, Debug};
use std::str::FromStr;
use itertools::Itertools;

macro_rules! t {
    ($e: expr) => {
        if let Ok(e) = $e {
            e
        } else {
            return None;
        }
    };
}

// order is reverted to derive Ord ;)
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Version {
    pub patch: usize,
    pub minor: usize,
    pub major: usize,
}

impl Version {
    pub fn new(major: usize, minor: usize, patch: usize) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl FromStr for Version {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.split('.')
            .collect_tuple()
            .and_then(|(major, minor, patch): (&str, &str, &str)| {
                Some(Version::new(
                    t!(major.parse()),
                    t!(minor.parse()),
                    t!(patch.parse()),
                ))
            })
            .ok_or(())
    }
}
