use std::fmt;
use std::io::Read;
use std::error::Error as StdError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;

use hyper;
use hyper::error::ParseError;
use hyper::client::response::Response;

/// Result type often returned from methods that can have `Error`s.
pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Http(hyper::Error),
    Uri(ParseError),
    Utf8(Utf8Error),
    Consul(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Http(ref err) => write!(f, "Http error: {}", err),
            Error::Uri(ref err) => write!(f, "Uri parse error: {}", err),
            Error::Utf8(ref err) => write!(f, "UTF8 error: {}", err),
            Error::Consul(ref err) => write!(f, "Consul response: {}", err),
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Http(ref err) => err.description(),
            Error::Uri(ref err) => err.description(),
            Error::Utf8(ref err) => err.description(),
            Error::Consul(ref err) => err,
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            Error::Http(ref err) => Some(err),
            Error::Uri(ref err) => Some(err),
            Error::Utf8(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<hyper::Error> for Error {
    fn from(err: hyper::Error) -> Error {
        Error::Http(err)
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Error {
        Error::Uri(err)
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Error {
        Error::Utf8(err)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        Error::Utf8(err.utf8_error())
    }
}

pub fn consul_error(mut resp: Response) -> Error {
    assert!(resp.status != hyper::Ok);
    let mut buf = String::new();
    resp.read_to_string(&mut buf).unwrap();
    Error::Consul(format!("{}: {}", resp.status.canonical_reason().unwrap(), buf))
}
