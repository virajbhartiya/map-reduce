use tokio::fs;
use std::path::Path;

pub async fn read_text_file(path: impl AsRef<Path>) -> std::io::Result<String> {
    fs::read_to_string(path).await
}

pub fn parse_server_url(url: &str) -> String {
    if !url.starts_with("http://") && !url.starts_with("https://") {
        format!("http://{}", url)
    } else {
        url.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_server_url() {
        assert_eq!(parse_server_url("localhost:50051"), "http://localhost:50051");
        assert_eq!(parse_server_url("http://localhost:50051"), "http://localhost:50051");
        assert_eq!(parse_server_url("https://localhost:50051"), "https://localhost:50051");
    }
}
