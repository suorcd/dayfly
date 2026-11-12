//! Read and merge S3 bucket lifecycle configuration documents.
//!
//! The document format is the S3 JSON form that
//! `docs/lifecycle-example.json` uses. Dayfly applies only the fields it
//! documents: an ID, a status, an optional prefix filter, an expiration
//! in whole days, and an abort-incomplete-upload age in whole days.
//!
//! A merge replaces a rule with the same ID and keeps every other rule.
//! This lets an operator add rules without a later apply deleting them.

use std::collections::HashSet;

use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus,
    LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
};
use serde::Deserialize;
use thiserror::Error;

/// An invalid or unsupported lifecycle document.
#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("cannot parse the lifecycle document: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("{0}")]
    Invalid(String),
    #[error("cannot build an S3 lifecycle rule: {0}")]
    Rule(String),
}

/// The S3 lifecycle document.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Document {
    #[serde(rename = "Rules")]
    pub rules: Vec<DocumentRule>,
}

/// One rule in the document.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentRule {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Status")]
    pub status: Option<String>,
    #[serde(rename = "Filter")]
    pub filter: Option<DocumentFilter>,
    #[serde(rename = "Expiration")]
    pub expiration: Option<DocumentExpiration>,
    #[serde(rename = "AbortIncompleteMultipartUpload")]
    pub abort: Option<DocumentAbort>,
}

/// The supported filter form: one prefix.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentFilter {
    #[serde(rename = "Prefix")]
    pub prefix: Option<String>,
}

/// The supported expiration form: whole days.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentExpiration {
    #[serde(rename = "Days")]
    pub days: Option<i64>,
}

/// The abort-incomplete-upload action.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentAbort {
    #[serde(rename = "DaysAfterInitiation")]
    pub days_after_initiation: Option<i64>,
}

/// The result of merging a document into the bucket rules.
#[derive(Debug)]
pub struct Merge {
    /// The complete rule set to write.
    pub rules: Vec<LifecycleRule>,
    /// Rules that the document did not name.
    pub kept: usize,
}

impl Document {
    /// Parse and check the structure of a document.
    pub fn parse(text: &str) -> Result<Self, LifecycleError> {
        let document: Self = serde_json::from_str(text)?;
        document.validate()?;
        Ok(document)
    }

    /// Check every rule and convert it to the S3 type.
    ///
    /// `max_ttl_days` is the longest TTL that the service accepts. An
    /// expiration at or below that value could delete a paste before its
    /// read-time expiry, so it is rejected unless
    /// `allow_short_expiration` is set.
    pub fn rules(
        &self,
        max_ttl_days: u32,
        allow_short_expiration: bool,
    ) -> Result<Vec<LifecycleRule>, LifecycleError> {
        if !allow_short_expiration {
            for rule in &self.rules {
                rule.check_expiration(max_ttl_days)?;
            }
        }
        self.rules.iter().map(DocumentRule::to_s3).collect()
    }

    fn validate(&self) -> Result<(), LifecycleError> {
        if self.rules.is_empty() {
            return Err(LifecycleError::Invalid(
                "the document has no rules".to_string(),
            ));
        }
        let mut ids = HashSet::new();
        for (index, rule) in self.rules.iter().enumerate() {
            let id = rule.id.trim();
            if id.is_empty() {
                return Err(LifecycleError::Invalid(format!(
                    "rule #{} has an empty ID",
                    index + 1
                )));
            }
            if !ids.insert(id.to_string()) {
                return Err(LifecycleError::Invalid(format!(
                    "rule ID {id:?} appears more than once"
                )));
            }
            if rule.expiration.is_none() && rule.abort.is_none() {
                return Err(LifecycleError::Invalid(format!(
                    "rule {id:?} has no action; set Expiration or AbortIncompleteMultipartUpload"
                )));
            }
            if let Some(expiration) = &rule.expiration {
                let Some(days) = expiration.days else {
                    return Err(LifecycleError::Invalid(format!(
                        "rule {id:?} has an Expiration without Days"
                    )));
                };
                if days <= 0 {
                    return Err(LifecycleError::Invalid(format!(
                        "rule {id:?} has an expiration of {days} days; it must be positive"
                    )));
                }
            }
            if let Some(abort) = &rule.abort {
                let Some(days) = abort.days_after_initiation else {
                    return Err(LifecycleError::Invalid(format!(
                        "rule {id:?} has AbortIncompleteMultipartUpload without DaysAfterInitiation"
                    )));
                };
                if days <= 0 {
                    return Err(LifecycleError::Invalid(format!(
                        "rule {id:?} aborts incomplete uploads after {days} days; it must be positive"
                    )));
                }
            }
            if let Some(status) = &rule.status {
                if status != "Enabled" && status != "Disabled" {
                    return Err(LifecycleError::Invalid(format!(
                        "rule {id:?} has status {status:?}; use Enabled or Disabled"
                    )));
                }
            }
        }
        Ok(())
    }
}

impl DocumentRule {
    fn check_expiration(&self, max_ttl_days: u32) -> Result<(), LifecycleError> {
        let Some(days) = self
            .expiration
            .as_ref()
            .and_then(|expiration| expiration.days)
        else {
            return Ok(());
        };
        if days <= i64::from(max_ttl_days) {
            return Err(LifecycleError::Invalid(format!(
                "rule {:?} expires after {days} days, but the maximum TTL is \
                 {max_ttl_days} days; an expiration must be longer than the maximum TTL",
                self.id.trim()
            )));
        }
        Ok(())
    }

    fn to_s3(&self) -> Result<LifecycleRule, LifecycleError> {
        let id = self.id.trim();
        let mut builder = LifecycleRule::builder()
            .id(id)
            .status(match self.status.as_deref() {
                Some("Disabled") => ExpirationStatus::Disabled,
                _ => ExpirationStatus::Enabled,
            })
            .filter(
                LifecycleRuleFilter::builder()
                    .prefix(
                        self.filter
                            .as_ref()
                            .and_then(|filter| filter.prefix.clone())
                            .unwrap_or_default(),
                    )
                    .build(),
            );

        if let Some(expiration) = &self.expiration {
            let days = expiration.days.unwrap_or_default();
            let days = i32::try_from(days).map_err(|_| {
                LifecycleError::Invalid(format!("rule {id:?} has an expiration that is too large"))
            })?;
            builder = builder.expiration(LifecycleExpiration::builder().days(days).build());
        }
        if let Some(abort) = &self.abort {
            let days = abort.days_after_initiation.unwrap_or_default();
            let days = i32::try_from(days).map_err(|_| {
                LifecycleError::Invalid(format!("rule {id:?} has an abort age that is too large"))
            })?;
            builder = builder.abort_incomplete_multipart_upload(
                AbortIncompleteMultipartUpload::builder()
                    .days_after_initiation(days)
                    .build(),
            );
        }

        builder
            .build()
            .map_err(|error| LifecycleError::Rule(error.to_string()))
    }
}

/// Merge configured rules into the current bucket rules by rule ID.
///
/// A configured rule replaces an existing rule with the same ID. Every
/// other existing rule stays. Existing rules with no ID also stay.
pub fn merge(existing: Vec<LifecycleRule>, configured: Vec<LifecycleRule>) -> Merge {
    let configured_ids: HashSet<&str> = configured.iter().filter_map(|rule| rule.id()).collect();
    let mut rules: Vec<LifecycleRule> = existing
        .into_iter()
        .filter(|rule| rule.id().is_none_or(|id| !configured_ids.contains(id)))
        .collect();
    let kept = rules.len();
    rules.extend(configured);
    Merge { rules, kept }
}

/// One line that describes a rule for CLI output.
pub fn describe(rule: &LifecycleRule) -> String {
    let status = match rule.status() {
        ExpirationStatus::Enabled => "Enabled",
        ExpirationStatus::Disabled => "Disabled",
        _ => "unknown",
    };
    let mut parts = vec![format!(
        "{}: prefix {:?}, status {status}",
        rule.id().unwrap_or(""),
        rule.filter()
            .and_then(|filter| filter.prefix())
            .unwrap_or("")
    )];
    if let Some(days) = rule.expiration().and_then(|expiration| expiration.days()) {
        parts.push(format!("expires after {days} days"));
    }
    if let Some(days) = rule
        .abort_incomplete_multipart_upload()
        .and_then(|abort| abort.days_after_initiation())
    {
        parts.push(format!("aborts incomplete uploads after {days} days"));
    }
    parts.join(", ")
}

/// Build the S3 request body from a rule set.
pub fn configuration(
    rules: Vec<LifecycleRule>,
) -> Result<BucketLifecycleConfiguration, LifecycleError> {
    BucketLifecycleConfiguration::builder()
        .set_rules(Some(rules))
        .build()
        .map_err(|error| LifecycleError::Rule(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = r#"{
        "Rules": [
            {
                "ID": "dayfly-expire-pastes",
                "Status": "Enabled",
                "Filter": { "Prefix": "pastes/" },
                "Expiration": { "Days": 90 },
                "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
            }
        ]
    }"#;

    fn rule(id: &str, days: i32) -> LifecycleRule {
        LifecycleRule::builder()
            .id(id)
            .status(ExpirationStatus::Enabled)
            .filter(
                LifecycleRuleFilter::builder()
                    .prefix("pastes/".to_string())
                    .build(),
            )
            .expiration(LifecycleExpiration::builder().days(days).build())
            .build()
            .expect("test rule must build")
    }

    #[test]
    fn parses_the_example_document() {
        let document = Document::parse(EXAMPLE).expect("example must parse");
        let rules = document.rules(3, false).expect("rules must convert");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].id(), Some("dayfly-expire-pastes"));
        assert_eq!(rules[0].status(), &ExpirationStatus::Enabled);
        assert_eq!(
            rules[0]
                .expiration()
                .and_then(|expiration| expiration.days()),
            Some(90)
        );
        assert_eq!(
            rules[0]
                .abort_incomplete_multipart_upload()
                .and_then(|abort| abort.days_after_initiation()),
            Some(7)
        );
    }

    #[test]
    fn the_example_document_passes_the_default_max_ttl_guard() {
        let document = Document::parse(include_str!("../docs/lifecycle-example.json"))
            .expect("the example must parse");
        let rules = document
            .rules(crate::config::MAX_TTL_DAYS, false)
            .expect("the example must pass the default max-TTL guard");
        let rule = rules
            .iter()
            .find(|rule| rule.filter().and_then(|filter| filter.prefix()) == Some("pastes/"))
            .expect("the example must keep the pastes/ rule");
        assert_eq!(
            rule.expiration().and_then(|expiration| expiration.days()),
            Some(3651)
        );
        assert_eq!(
            rule.abort_incomplete_multipart_upload()
                .and_then(|abort| abort.days_after_initiation()),
            Some(7)
        );
        let words = rules
            .iter()
            .find(|rule| rule.filter().and_then(|filter| filter.prefix()) == Some("words/"))
            .expect("the example must have a words/ rule");
        assert_eq!(
            words.expiration().and_then(|expiration| expiration.days()),
            Some(3651)
        );
        assert_eq!(words.status(), &ExpirationStatus::Enabled);
    }

    #[test]
    fn rejects_an_expiration_at_or_below_the_max_ttl() {
        let mut document = Document::parse(EXAMPLE).expect("example must parse");
        document.rules[0].expiration = Some(DocumentExpiration { days: Some(3) });
        let error = document
            .rules(3, false)
            .expect_err("an expiration at the maximum TTL must fail");
        assert!(
            error.to_string().contains("longer than the maximum TTL"),
            "got: {error}"
        );
    }

    #[test]
    fn allows_a_short_expiration_only_when_asked() {
        let document = Document::parse(EXAMPLE).expect("example must parse");
        assert!(
            document.rules(90, false).is_err(),
            "the guard must reject an expiration at the maximum TTL"
        );
        let rules = document
            .rules(90, true)
            .expect("the override must accept the rule");
        assert_eq!(rules.len(), 1);
    }

    #[test]
    fn rejects_duplicate_ids() {
        let text = r#"{"Rules":[
            {"ID":"a","Expiration":{"Days":90}},
            {"ID":"a","Expiration":{"Days":91}}
        ]}"#;
        let error = Document::parse(text).expect_err("duplicate IDs must fail");
        assert!(error.to_string().contains("more than once"), "got: {error}");
    }

    #[test]
    fn rejects_a_rule_without_an_action() {
        let text = r#"{"Rules":[{"ID":"a"}]}"#;
        let error = Document::parse(text).expect_err("a rule with no action must fail");
        assert!(error.to_string().contains("no action"), "got: {error}");
    }

    #[test]
    fn rejects_an_unsupported_expiration_date() {
        let text = r#"{"Rules":[
            {"ID":"a","Expiration":{"Date":"2030-01-01"}}
        ]}"#;
        assert!(
            Document::parse(text).is_err(),
            "a date expiration is not supported"
        );
    }

    #[test]
    fn merge_replaces_a_rule_with_the_same_id() {
        let merged = merge(
            vec![rule("a", 90), rule("b", 90)],
            vec![rule("b", 120), rule("c", 90)],
        );
        let ids: Vec<_> = merged.rules.iter().filter_map(|rule| rule.id()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
        assert_eq!(merged.kept, 1);
        let b = merged
            .rules
            .iter()
            .find(|rule| rule.id() == Some("b"))
            .and_then(|rule| rule.expiration())
            .and_then(|expiration| expiration.days());
        assert_eq!(b, Some(120));
    }
}
