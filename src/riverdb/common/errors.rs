use std::io;
use std::sync;

use serde_yaml;

use custom_error::custom_error;

custom_error!{Error,
    StrError{msg: &'static str} = "{msg}",
    Io{source: io::Error} = "io error",
    Yaml{source: serde_yaml::Error} = "yaml error",
    PosionError{source: sync::PoisonError} = "poison error",
}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

