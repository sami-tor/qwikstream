use anyhow::{anyhow, Result};

pub fn parse_byte_size(input: &str) -> Result<u64> {
    let trimmed = input.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(anyhow!("size cannot be empty"));
    }

    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split_at);
    if number.is_empty() {
        return Err(anyhow!("size must start with a number"));
    }

    let value: u64 = number.parse()?;
    let multiplier = match unit.trim() {
        "" | "b" => 1,
        "kb" | "k" => 1024,
        "mb" | "m" => 1024 * 1024,
        "gb" | "g" => 1024 * 1024 * 1024,
        "tb" | "t" => 1024_u64.pow(4),
        other => return Err(anyhow!("unsupported size unit: {other}")),
    };

    value
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow!("size is too large"))
}

pub fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    const TB: f64 = GB * 1024.0;

    let value = bytes as f64;
    if value >= TB {
        format!("{:.2} TB", value / TB)
    } else if value >= GB {
        format!("{:.2} GB", value / GB)
    } else if value >= MB {
        format!("{:.2} MB", value / MB)
    } else if value >= KB {
        format!("{:.2} KB", value / KB)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_bytes() {
        assert_eq!(parse_byte_size("42").unwrap(), 42);
    }

    #[test]
    fn parses_common_units_case_insensitively() {
        assert_eq!(parse_byte_size("1kb").unwrap(), 1024);
        assert_eq!(parse_byte_size("2MB").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_byte_size("3g").unwrap(), 3 * 1024 * 1024 * 1024);
    }

    #[test]
    fn rejects_invalid_units() {
        let err = parse_byte_size("12xb").unwrap_err().to_string();
        assert!(err.contains("unsupported size unit"));
    }

    #[test]
    fn formats_bytes_for_display() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
    }
}
