use std::ffi::OsStr;

pub fn os_str_is_off(value: &OsStr) -> bool {
    value
        .to_str()
        .map(|s| {
            matches!(
                s.trim().to_ascii_lowercase().as_str(),
                "off" | "false" | "0" | "none"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use crate::os_str_is_off;
    use std::ffi::OsStr;

    #[test]
    fn is_off_recognizes_disable_values() {
        for v in ["off", "OFF", "Off", "false", "0", "none", "  off  "] {
            assert!(os_str_is_off(OsStr::new(v)), "{v:?} should disable");
        }
        for v in ["on", "1", "true", "/tmp/logs/x.log"] {
            assert!(!os_str_is_off(OsStr::new(v)), "{v:?} should not disable");
        }
    }
}
