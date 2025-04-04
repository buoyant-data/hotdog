use tracing::log::*;

/// Enum of syslog parse related errors
#[derive(Debug)]
pub enum SyslogErrors {
    UnknownFormat,
}

/// SyslogMessage is just a wrapper struct to allow us to deserialize RFC 5424 and RFC 3164 syslog
/// messages into some format that can be passed throughout hotdog
#[derive(Debug)]
pub struct SyslogMessage {
    pub msg: String,
    pub severity: Option<String>,
    pub facility: Option<String>,
    pub hostname: Option<String>,
    pub appname: Option<String>,
}

/// Attempt to parse a given line either as RFC 5424 or RFC 3164
pub fn parse_line(line: String) -> std::result::Result<SyslogMessage, SyslogErrors> {
    match syslog_rfc5424::parse_message(&line) {
        Ok(msg) => {
            let wrapped = SyslogMessage {
                msg: msg.msg,
                severity: Some(msg.severity.as_str().to_string()),
                facility: Some(msg.facility.as_str().to_string()),
                hostname: msg.hostname,
                appname: msg.appname,
            };
            Ok(wrapped)
        }
        Err(_) => {
            let parsed = syslog_loose::parse_message(&line, syslog_loose::Variant::Either);

            /*
             * Since syslog_loose doesn't give a Result, the only way to tell if themessage wasn't
             * parsed properly is if some fields are None'd out.
             */
            if parsed.timestamp.is_some() {
                let wrapped = SyslogMessage {
                    msg: parsed.msg.to_string(),
                    severity: parsed
                        .severity
                        .map_or_else(|| None, |s| Some(s.as_str().to_string())),
                    facility: parsed
                        .facility
                        .map_or_else(|| None, |f| Some(f.as_str().to_string())),
                    hostname: parsed
                        .hostname
                        .map_or_else(|| None, |h| Some(h.to_string())),
                    appname: parsed.appname.map_or_else(|| None, |a| Some(a.to_string())),
                };
                return Ok(wrapped);
            }
            warn!("Message received we cannot parse: {}", line);
            Err(SyslogErrors::UnknownFormat)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsing_invalid() {
        let buffer = "blah".to_string();
        let parsed = parse_line(buffer);
        if let Ok(msg) = &parsed {
            println!("msg: {}", msg.msg);
        }
        assert!(parsed.is_err());
    }

    #[test]
    fn test_5424() {
        let buffer = r#"<13>1 2020-04-18T15:16:09.956153-07:00 coconut tyler - - [timeQuality tzKnown="1" isSynced="1" syncAccuracy="505061"] hi"#.to_string();
        let parsed = parse_line(buffer);
        assert!(parsed.is_ok());
        if let Ok(msg) = parsed {
            assert_eq!("hi", msg.msg);
            assert_eq!(Some("coconut".to_string()), msg.hostname);
            assert_eq!(Some("user".to_string()), msg.facility);
            assert_eq!(Some("notice".to_string()), msg.severity);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_3164() {
        let buffer = r#"<190>May 13 21:45:18 coconut hotdog: hi"#.to_string();
        let parsed = parse_line(buffer);
        assert!(parsed.is_ok());
        if let Ok(msg) = parsed {
            assert_eq!("hi", msg.msg);
            assert_eq!(Some("coconut".to_string()), msg.hostname);
            assert_eq!(Some("hotdog".to_string()), msg.appname);
            assert_eq!(Some("local7".to_string()), msg.facility);
            assert_eq!(Some("info".to_string()), msg.severity);
        } else {
            assert!(false);
        }
    }
}
