//! A submodule for pypi source that implements python package version semantics.
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use nom::branch::alt;
use nom::bytes::complete::{is_a, tag_no_case};
use nom::character::complete;
use nom::character::complete::{char, one_of};
use nom::combinator::{all_consuming, opt, success};
use nom::error::Error;
use nom::multi::{many0, separated_list1};
use nom::sequence::{preceded, terminated, tuple};
use nom::{Finish, IResult, Parser};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Version {
    pub epoch: u32,
    pub chunks: Vec<u32>,
    pub pre: Option<PreRelease>,
    pub post: Option<u32>,
    pub dev: Option<u32>,
    pub local: Option<String>,
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn rev_option_ord<T: Ord>(lhs: &Option<T>, rhs: &Option<T>) -> Ordering {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => lhs.cmp(rhs),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.epoch
            .cmp(&other.epoch)
            .then_with(|| self.chunks.cmp(&other.chunks))
            .then_with(|| rev_option_ord(&self.pre, &other.pre))
            .then_with(|| self.post.cmp(&other.post))
            .then_with(|| rev_option_ord(&self.dev, &other.dev))
            .then_with(|| self.local.cmp(&other.local))
    }
}

impl Version {
    /// Parse a version string.
    ///
    /// # Errors
    ///
    /// Returns an error if the string is not a valid version.
    pub fn parse(s: &str) -> Result<Self, Error<&str>> {
        let (_, version) = all_consuming(version).parse(s).finish()?;
        Ok(version)
    }
    /// Returns true if this version is stable.
    pub const fn is_stable(&self) -> bool {
        self.pre.is_none() && self.dev.is_none()
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.epoch > 0 {
            write!(f, "{}!", self.epoch)?;
        }
        for (i, chunk) in self.chunks.iter().enumerate() {
            if i > 0 {
                write!(f, ".")?;
            }
            write!(f, "{}", chunk)?;
        }
        if let Some(pre) = &self.pre {
            write!(f, "{}", pre)?;
        }
        if let Some(post) = &self.post {
            write!(f, ".post{}", post)?;
        }
        if let Some(dev) = &self.dev {
            write!(f, ".dev{}", dev)?;
        }
        if let Some(local) = &self.local {
            write!(f, "+{}", local)?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum PreRelease {
    Alpha(u32),
    Beta(u32),
    RC(u32),
}

impl Display for PreRelease {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Alpha(v) => write!(f, "a{}", v),
            Self::Beta(v) => write!(f, "b{}", v),
            Self::RC(v) => write!(f, "rc{}", v),
        }
    }
}

fn pre_release(input: &str) -> IResult<&str, PreRelease> {
    let (input, _) = opt(one_of("._-")).parse(input)?;

    let alpha = alt((tag_no_case("alpha"), tag_no_case("a")));
    let beta = alt((tag_no_case("beta"), tag_no_case("b")));
    let rc = alt((
        tag_no_case("rc"),
        tag_no_case("c"),
        tag_no_case("preview"),
        tag_no_case("pre"),
    ));
    let (input, f) = alt((
        alpha.map(|_| PreRelease::Alpha as fn(u32) -> PreRelease),
        beta.map(|_| PreRelease::Beta as fn(u32) -> PreRelease),
        rc.map(|_| PreRelease::RC as fn(u32) -> PreRelease),
    ))
    .parse(input)?;

    let (input, version) = preceded(opt(one_of("._-")), complete::u32)
        .or(success(0))
        .parse(input)?;

    Ok((input, f(version)))
}

fn post_release(input: &str) -> IResult<&str, u32> {
    let (input, sep) = opt(one_of("._-")).parse(input)?;
    let (input, has_tag) =
        match alt((tag_no_case("post"), tag_no_case("rev"), tag_no_case("r"))).parse(input) {
            Ok((input, _)) => (input, true),
            Err(_) if sep == Some('-') => (input, false),
            Err(e) => return Err(e),
        };
    let (input, version) = match preceded(opt(one_of("._-")), complete::u32).parse(input) {
        Ok((input, version)) => (input, version),
        Err(_) if has_tag => (input, 0),
        Err(e) => return Err(e),
    };
    Ok((input, version))
}

fn dev_release(input: &str) -> IResult<&str, u32> {
    let (input, _) = opt(one_of("._-")).parse(input)?;
    let (input, _) = tag_no_case("dev").parse(input)?;

    complete::u32.or(success(0)).parse(input)
}

fn version(input: &str) -> IResult<&str, Version> {
    let epoch = terminated(complete::u32, char('!')).or(success(0));
    let chunks = separated_list1(char('.'), complete::u32);
    let local = preceded(
        char('+'),
        is_a("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_."),
    );

    let trim = || many0(one_of(" \t\n\r\u{0b}\u{0c}"));

    let (input, version) = tuple((
        trim(),
        opt(char('v')),
        epoch,
        chunks,
        opt(pre_release),
        opt(post_release),
        opt(dev_release),
        opt(local),
        trim(),
    ))
    .map(|(_, _, epoch, mut chunks, pre, post, dev, local, _)| {
        if let Some(non_zero) = chunks.iter().rposition(|&v| v != 0) {
            chunks.truncate(non_zero + 1);
        }
        Version {
            epoch,
            chunks,
            pre,
            post,
            dev,
            local: local.map(|s| s.replace(['-', '_'], ".")),
        }
    })
    .parse(input)?;
    Ok((input, version))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::Version;

    #[rstest]
    #[case("0")]
    #[case("0.9")]
    #[case("0.009")]
    #[case("0.9.0.0")]
    #[case("1!1.0")]
    #[case("1.1RC1")]
    #[case("1.1a1")]
    #[case("1.1a.1")]
    #[case("1.1.a1")]
    #[case("1.1-a1")]
    #[case("1.1_a1")]
    #[case("1.1b1")]
    #[case("1.1rc1")]
    #[case("1.1alpha1")]
    #[case("1.1beta2")]
    #[case("1.1c3")]
    #[case("1.1pre3")]
    #[case("1.1preview3")]
    #[case("1.2a")]
    #[case("1.2.post2")]
    #[case("1.2-post2")]
    #[case("1.2_post2")]
    #[case("1.2post2")]
    #[case("1.2-post-2")]
    #[case("1.0-r4")]
    #[case("1.0-rev4")]
    #[case("1.0-1")]
    #[case("1.0pre-1-1")]
    #[case("1.0-")]
    #[case("1.0_1")]
    #[case(" \tv1.0 \n ")]
    #[case("1.dev0")]
    #[case("1.0.dev456")]
    #[case("1.0a1")]
    #[case("1.0a2.dev456")]
    #[case("1.0a12.dev456")]
    #[case("1.0a12")]
    #[case("1.0b1.dev456")]
    #[case("1.0b2")]
    #[case("1.0b2.post345.dev456")]
    #[case("1.0b2.post345")]
    #[case("1.0rc1.dev456")]
    #[case("1.0rc1")]
    #[case("1.0")]
    #[case("1.0+abc.5")]
    #[case("1.0+abc.7")]
    #[case("1.0+5")]
    #[case("1.0.post456.dev34")]
    #[case("1.0.post456")]
    #[case("1.0.15")]
    #[case("1.1.dev1")]
    fn test_parse(#[case] input: &str) {
        insta::with_settings!({
            snapshot_suffix => input,
        }, {
            insta::assert_debug_snapshot!(Version::parse(input));
        });
    }

    #[rstest]
    #[case("1.0", true)]
    #[case("1.0a1", false)]
    #[case("1.0.dev1", false)]
    #[case("1.0a1.post1", false)]
    #[case("1.0.post1.dev1", false)]
    #[case("1.0.post1", true)]
    #[case("1.0+ubuntu-1", true)]
    fn test_stable(#[case] input: &str, #[case] expect: bool) {
        assert_eq!(Version::parse(input).unwrap().is_stable(), expect);
    }

    #[test]
    fn test_ord() {
        let versions = [
            "1!1.0",
            "0.1",
            "0.2",
            "1.0",
            "1.0.1",
            "1.0.1.1",
            "1.0.2",
            "1.0.2.1",
            "1.0a1",
            "1.0rc1.post1",
            "1.0b1",
            "1.0b1.dev2",
            "1.0rc1",
            "1.0.post1",
            "1.0.post1.dev2",
            "1.0+ubuntu-1",
            "1.0b2+ubuntu-1",
        ];
        let mut versions: Vec<_> = versions
            .iter()
            .map(|s| Version::parse(s).unwrap())
            .collect();
        versions.sort();
        let versions: Vec<_> = versions.into_iter().map(|v| v.to_string()).collect();
        insta::assert_debug_snapshot!(versions);
    }
}
