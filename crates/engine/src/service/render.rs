use crate::error::{EngineError, EngineResult};

/// Render a `{{KEY}}` template. Any placeholder left after substitution is
/// an error — this catches template/variable drift at install time (and in
/// unit tests) instead of shipping a broken service definition.
pub fn render(template: &str, vars: &[(&str, &str)]) -> EngineResult<String> {
    let mut out = template.to_string();
    for (key, value) in vars {
        let needle = format!("{{{{{key}}}}}");
        out = out.replace(&needle, value);
    }
    if out.contains("{{") {
        let leftovers: Vec<&str> = out
            .lines()
            .filter(|line| line.contains("{{"))
            .map(str::trim)
            .collect();
        return Err(EngineError::Service(format!(
            "service template contains unreplaced placeholders: {}",
            leftovers.join(" | ")
        )));
    }
    Ok(out)
}

/// Minimal escaping for text nodes in the launchd plist.
pub fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_every_occurrence() {
        let out = render("a {{X}} b {{X}} {{Y}}", &[("X", "1"), ("Y", "2")]).unwrap();
        assert_eq!(out, "a 1 b 1 2");
    }

    #[test]
    fn leftover_placeholders_are_an_error() {
        let err = render("hello {{MISSING}}", &[]).unwrap_err();
        assert!(err.to_string().contains("MISSING"), "{err}");
    }

    #[test]
    fn xml_escape_handles_ampersand_first() {
        assert_eq!(xml_escape("a&b<c>d"), "a&amp;b&lt;c&gt;d");
        assert_eq!(xml_escape("&lt;"), "&amp;lt;");
    }

    #[test]
    fn launchd_template_renders_without_leftovers() {
        let out = render(
            super::super::launchd_template(),
            &[
                ("LABEL", "io.valqeron.engine"),
                ("EXE", "/usr/local/bin/valqeron-engine"),
                ("STDOUT_PATH", "/tmp/out.log"),
                ("STDERR_PATH", "/tmp/err.log"),
                ("ENV_BLOCK", ""),
            ],
        )
        .unwrap();
        assert!(out.contains("<string>/usr/local/bin/valqeron-engine</string>"));
        assert!(out.contains("SuccessfulExit"));
        assert!(!out.contains("{{"));
    }

    #[test]
    fn systemd_template_renders_without_leftovers() {
        let out = render(
            super::super::systemd_template(),
            &[
                ("EXE", "/usr/local/bin/valqeron-engine"),
                ("RW_PATHS", "\"/data\" \"/logs\""),
                ("ENV_BLOCK", ""),
            ],
        )
        .unwrap();
        assert!(out.contains("Restart=on-failure"));
        assert!(out.contains("WantedBy=default.target"));
        assert!(!out.contains("{{"));
    }

    #[test]
    fn templates_contain_no_developer_machine_paths() {
        for template in [
            super::super::launchd_template(),
            super::super::systemd_template(),
        ] {
            assert!(
                !template.contains("/Users/") && !template.contains("/home/"),
                "template must not hardcode a developer path"
            );
        }
    }
}
