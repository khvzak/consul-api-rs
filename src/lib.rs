extern crate hyper;
extern crate serde;
extern crate serde_json;
extern crate base64;

pub mod agent;
pub mod keyvalue;
pub mod error;

pub use agent::Agent;
pub use keyvalue::KeyValue;

pub use error::Result;

pub struct Consul {
    address: String,
    client: hyper::Client,
}

impl Consul {
    pub fn new<S>(address: S) -> Self where S: Into<String> {
        Consul { address: address.into(), client: hyper::Client::new() }
    }

    pub fn default() -> Self {
        Self::new("127.0.0.1:8500")
    }

    pub fn agent(&self) -> Agent {
        Agent::new(self)
    }

    pub fn kv(&self) -> KeyValue {
        KeyValue::new(self)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
