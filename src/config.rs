use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone, Default)]
pub struct QuokkaConfig {
    pub flaky_retry: Option<FlakyRetryConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FlakyRetryConfig {
    pub attempts: u32,
}

pub fn load_config() -> QuokkaConfig {
    let home = std::env::var("HOME").ok().map(PathBuf::from);
    load_config_from_home(home)
}

pub fn load_config_from_home(home: Option<PathBuf>) -> QuokkaConfig {
    if let Some(home) = home {
        let path = home.join(".quokka/config.toml");
        if path.exists() {
            if let Ok(content) = std::fs::read_to_string(&path) {
                if let Ok(config) = toml::from_str::<QuokkaConfig>(&content) {
                    return config;
                }
            }
        }
    }
    QuokkaConfig::default()
}
