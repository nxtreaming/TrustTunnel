use std::net::IpAddr;
use serde::{Deserialize, Serialize};
use ipnet::IpNet;

/// Action to take when a rule matches
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RuleAction {
    Allow,
    Deny,
}

/// Individual filter rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    /// CIDR range to match against client IP
    #[serde(default)]
    pub cidr: Option<String>,
    
    /// Client random prefix to match (hex-encoded)
    /// Can optionally include a mask in format: "prefix[/mask]" (e.g., "aabbcc/ff00ff")
    /// If mask is specified, matching uses: client_random & mask == prefix & mask
    /// If no mask, uses prefix matching
    #[serde(default)]
    pub client_random_prefix: Option<String>,
    
    /// Action to take when this rule matches
    pub action: RuleAction,
}

/// Rules configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RulesConfig {
    /// List of filter rules
    #[serde(default)]
    pub rule: Vec<Rule>,
}

/// Rule evaluation engine
pub struct RulesEngine {
    rules: RulesConfig,
}

/// Result of rule evaluation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleEvaluation {
    Allow,
    Deny,
}

impl Rule {
    /// Check if this rule matches the given connection parameters
    pub fn matches(&self, client_ip: &IpAddr, client_random: Option<&[u8]>) -> bool {
        let mut matches = true;
        
        // Check CIDR match if specified
        if let Some(cidr_str) = &self.cidr {
            if let Ok(cidr) = cidr_str.parse::<IpNet>() {
                matches &= cidr.contains(client_ip);
            } else {
                // Invalid CIDR, rule doesn't match
                return false;
            }
        }
        
        // Check client_random prefix if specified
        if let Some(prefix_str) = &self.client_random_prefix {
            if let Some(client_random_data) = client_random {
                // Check if mask is specified in format "prefix[/mask]"
                if let Some(slash_pos) = prefix_str.find('/') {
                    // Parse prefix and mask separately
                    let (prefix_part, mask_part) = prefix_str.split_at(slash_pos);
                    let mask_part = &mask_part[1..]; // Skip the '/'
                    
                    if let (Ok(prefix_bytes), Ok(mask_bytes)) = (hex::decode(prefix_part), hex::decode(mask_part)) {
                        // Apply mask: client_random & mask == prefix & mask
                        let mask_len = mask_bytes.len().min(prefix_bytes.len()).min(client_random_data.len());
                        let mut masked_match = mask_len > 0;
                        
                        for i in 0..mask_len {
                            if (client_random_data[i] & mask_bytes[i]) != (prefix_bytes[i] & mask_bytes[i]) {
                                masked_match = false;
                                break;
                            }
                        }
                        
                        matches &= masked_match;
                    } else {
                        // Invalid hex in prefix or mask, rule doesn't match
                        return false;
                    }
                } else {
                    // No mask, use simple prefix matching
                    if let Ok(prefix_bytes) = hex::decode(prefix_str) {
                        matches &= client_random_data.starts_with(&prefix_bytes);
                    } else {
                        // Invalid hex prefix, rule doesn't match
                        return false;
                    }
                }
            } else {
                // No client_random provided but rule requires it, doesn't match
                matches = false;
            }
        }
        
        matches
    }
}

impl RulesEngine {
    /// Create a new rules engine from rules config
    pub fn from_config(rules: RulesConfig) -> Self {
        Self { rules }
    }
    
    /// Create a default rules engine that allows all connections
    pub fn default_allow() -> Self {
        Self {
            rules: RulesConfig { rule: vec![] }
        }
    }
    
    /// Evaluate connection against all rules
    /// Returns the action from the first matching rule, or Allow if no rules match
    pub fn evaluate(&self, client_ip: &IpAddr, client_random: Option<&[u8]>) -> RuleEvaluation {
        for rule in &self.rules.rule {
            if rule.matches(client_ip, client_random) {
                return match rule.action {
                    RuleAction::Allow => RuleEvaluation::Allow,
                    RuleAction::Deny => RuleEvaluation::Deny,
                };
            }
        }
        
        // Default action if no rules match: allow
        RuleEvaluation::Allow
    }
    
    /// Get a reference to the rules configuration
    pub fn config(&self) -> &RulesConfig {
        &self.rules
    }
}

impl Default for RulesConfig {
    fn default() -> Self {
        Self { rule: vec![] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    
    #[test]
    fn test_cidr_rule_matching() {
        let rule = Rule {
            cidr: Some("192.168.1.0/24".to_string()),
            client_random_prefix: None,
            action: RuleAction::Allow,
        };
        
        let ip_match = IpAddr::from_str("192.168.1.100").unwrap();
        let ip_no_match = IpAddr::from_str("10.0.0.1").unwrap();
        
        assert!(rule.matches(&ip_match, None));
        assert!(!rule.matches(&ip_no_match, None));
    }
    
    #[test]
    fn test_client_random_prefix_matching() {
        let rule = Rule {
            cidr: None,
            client_random_prefix: Some("aabbcc".to_string()),
            action: RuleAction::Deny,
        };
        
        let client_random_match = hex::decode("aabbccddee").unwrap();
        let client_random_no_match = hex::decode("112233").unwrap();
        
        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        
        assert!(rule.matches(&ip, Some(&client_random_match)));
        assert!(!rule.matches(&ip, Some(&client_random_no_match)));
        assert!(!rule.matches(&ip, None)); // No client random provided
    }
    
    #[test]
    fn test_combined_rule_matching() {
        let rule = Rule {
            cidr: Some("10.0.0.0/8".to_string()),
            client_random_prefix: Some("ff".to_string()),
            action: RuleAction::Allow,
        };
        
        let ip_match = IpAddr::from_str("10.1.2.3").unwrap();
        let ip_no_match = IpAddr::from_str("192.168.1.1").unwrap();
        let client_random_match = hex::decode("ff00112233").unwrap();
        let client_random_no_match = hex::decode("0011223344").unwrap();
        
        // Both must match
        assert!(rule.matches(&ip_match, Some(&client_random_match)));
        assert!(!rule.matches(&ip_match, Some(&client_random_no_match)));
        assert!(!rule.matches(&ip_no_match, Some(&client_random_match)));
        assert!(!rule.matches(&ip_no_match, Some(&client_random_no_match)));
    }
    
    #[test]
    fn test_rules_engine_evaluation() {
        let rules = RulesConfig {
            rule: vec![
                Rule {
                    cidr: Some("192.168.1.0/24".to_string()),
                    client_random_prefix: None,
                    action: RuleAction::Deny,
                },
                Rule {
                    cidr: Some("10.0.0.0/8".to_string()),
                    client_random_prefix: None,
                    action: RuleAction::Allow,
                },
                Rule {
                    cidr: None,
                    client_random_prefix: None,
                    action: RuleAction::Deny, // Catch-all deny
                },
            ],
        };
        
        let engine = RulesEngine::from_config(rules);
        
        let ip_deny = IpAddr::from_str("192.168.1.100").unwrap();
        let ip_allow = IpAddr::from_str("10.1.2.3").unwrap();
        let ip_default = IpAddr::from_str("172.16.1.1").unwrap();
        
        assert_eq!(engine.evaluate(&ip_deny, None), RuleEvaluation::Deny);
        assert_eq!(engine.evaluate(&ip_allow, None), RuleEvaluation::Allow);
        assert_eq!(engine.evaluate(&ip_default, None), RuleEvaluation::Deny); // Default deny
    }
    
    #[test]
    fn test_client_random_mask_matching() {
        // Test mask matching: only check specific bits
        // Format: "prefix/mask" where mask 0xf0f0 means we only care about bits in positions where mask is 1
        let rule = Rule {
            cidr: None,
            client_random_prefix: Some("a0b0/f0f0".to_string()), // prefix=a0b0, mask=f0f0
            action: RuleAction::Allow,
        };
        
        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        
        // Should match: a5b5 & f0f0 = a0b0, same as prefix & mask
        let client_random_match1 = hex::decode("a5b5ccdd").unwrap(); // 10100101 10110101
        // Should match: a9bf & f0f0 = a0b0, same as prefix & mask  
        let client_random_match2 = hex::decode("a9bfeeaa").unwrap(); // 10101001 10111111
        // Should not match: b0b0 & f0f0 = b0b0, different from a0b0
        let client_random_no_match1 = hex::decode("b0b01122").unwrap(); // 10110000 10110000
        // Should not match: a0c0 & f0f0 = a0c0, different from a0b0
        let client_random_no_match2 = hex::decode("a0c03344").unwrap(); // 10100000 11000000
        
        assert!(rule.matches(&ip, Some(&client_random_match1)));
        assert!(rule.matches(&ip, Some(&client_random_match2)));
        assert!(!rule.matches(&ip, Some(&client_random_no_match1)));
        assert!(!rule.matches(&ip, Some(&client_random_no_match2)));
    }
    
    #[test]
    fn test_client_random_mask_full_bytes() {
        // Test with full byte mask - only first 2 bytes matter
        let rule = Rule {
            cidr: None,
            client_random_prefix: Some("12345678/ffff0000".to_string()),
            action: RuleAction::Allow,
        };
        
        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        
        // Should match: first 2 bytes are 0x1234, last 2 can be anything
        let client_random_match = hex::decode("1234aaaabbbb").unwrap();
        // Should not match: first 2 bytes are 0x1233
        let client_random_no_match = hex::decode("12335678ccdd").unwrap();
        
        assert!(rule.matches(&ip, Some(&client_random_match)));
        assert!(!rule.matches(&ip, Some(&client_random_no_match)));
    }
    
    #[test]
    fn test_client_random_invalid_mask_format() {
        // Test that invalid format "prefix/" (slash without mask) doesn't match
        let rule = Rule {
            cidr: None,
            client_random_prefix: Some("aabbcc/".to_string()), // Invalid: empty mask
            action: RuleAction::Allow,
        };
        
        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        let client_random = hex::decode("aabbccddee").unwrap();
        
        // Should not match due to invalid format
        assert!(!rule.matches(&ip, Some(&client_random)));
    }
}
