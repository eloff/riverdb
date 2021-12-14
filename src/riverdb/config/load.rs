use std::path::{Path, PathBuf};
use tracing::{info_span, info, debug};
use std::env;
use std::borrow::Cow;
use regex::{Regex, Captures};

use crate::riverdb::{Error, Result};
use crate::riverdb::config::config;


/// Load configuration settings from riverdb.yaml
/// Searching in order:
/// 1) config_path passed as first command line argument
/// 2) Current directory
/// 3) Any parent directory of the current directory, up to root
/// 4) ~/.config/riverdb/
/// 5) ~/
/// 6) /etc/riverdb/
///
/// This replaces ${ENV_VAR[:DEFAULT]} parameters in the yaml file with values from the environment
/// variable, if set, otherwise, optionally with the given default value after the :
pub fn load_config(config_name: &str) -> Result<&'static config::Settings> {
    let _span = info_span!("loading config file");
    let config_path = find_config_file(config_name)?;
    info!(config_path = %config_path.to_string_lossy().into_owned(), "found config file");
    let raw_yaml = std::fs::read_to_string(&config_path)?;
    let yaml_text = replace_env_vars(&raw_yaml)?;

    let config = unsafe { &mut *config::SETTINGS.as_mut_ptr() };
    *config = serde_yaml::from_str(&yaml_text)?;
    config.load(config_path)?;
    Ok(&*config)
}

fn find_config_file(config_name: &str) -> Result<PathBuf> {
    // Use the full path given as the first command line argument
    if let Some(path) = env::args().skip(1).next() {
        debug!("using config_path passed on command line");
        return Ok(PathBuf::from(path));
    }

    // Check the current directory or any of its parents for config_name
    if let Ok(start) = env::current_dir() {
        let mut dir = start.as_path();
        while !dir.as_os_str().is_empty() {
            debug!("checking for config file in {}", dir.to_string_lossy());
            let fp = Path::join(dir, config_name);
            if fp.exists() {
                return Ok(fp);
            }
            if let Some(parent) = dir.parent() {
                dir = parent;
            } else {
                break;
            }
        }
    }

    // Check  ~/.config/riverdb/{config_name}
    let mut conf_path = Path::join(Path::new(".config/riverdb"), config_name);
    // HOME is required to be set on POSIX systems, but if it's not set we'll try ~/
    let home = env::var("HOME").unwrap_or_else(|_| "~/".to_string());
    conf_path = Path::join(Path::new(&home), conf_path);
    debug!("checking for config file in {}", conf_path.to_string_lossy());
    if conf_path.exists() {
        return Ok(conf_path);
    }

    // Check ~/.{config_name}
    conf_path = Path::join(Path::new(&home), ".".to_string() + config_name);
    debug!("checking for config file in {}", conf_path.to_string_lossy());
    if conf_path.exists() {
        return Ok(conf_path);
    }

    // Check /etc/riverdb/{config_name}
    conf_path = Path::join(Path::new("/etc/riverdb"), config_name);
    debug!("checking for config file in {}", conf_path.to_string_lossy());
    if conf_path.exists() {
        return Ok(conf_path);
    }

    Err(Error::new(format!("config file {} not found", config_name)))
}

fn replace_env_vars(raw_yaml: &str) -> Result<Cow<str>> {
    // We only call this function once and then never again, so don't keep the regex
    let re_var = Regex::new(r"\$\{([a-zA-Z_][0-9a-zA-Z_]*)(?::([^}]+?))?\}").unwrap();

    let mut errors = Vec::<String>::new();

    let replaced_text = re_var.replace_all(&raw_yaml, |caps: &Captures| {
        match env::var(&caps[1]) {
            Ok(val) => val,
            Err(_) => {
                if let Some(default) = caps.get(2) {
                    let s = default.as_str();
                    if s.starts_with("?") {
                        errors.push((&s[1..]).to_string());
                        ""
                    } else {
                        default.as_str()
                    }.to_string()
                } else {
                    errors.push(format!("environment variable {} is required but not defined", &caps[1]));
                    "".to_string()
                }
            }
        }
    });

    if errors.is_empty() {
        Ok(replaced_text)
    } else {
        Err(Error::new(errors.join("\n")))
    }
}