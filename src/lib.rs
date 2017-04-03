extern crate hyper;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate base64;

pub mod agent;
pub mod keyvalue;
pub mod error;

pub use agent::{Agent, AgentCheck, AgentCheckRegistration, AgentService, AgentServiceRegistration};
pub use keyvalue::KeyValue;

pub use error::Result;

use hyper::Url;
use hyper::client::RequestBuilder;
use hyper::method::Method;

pub use serde_json::Value as JValue;

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

    pub fn _request1(&self, method: Method, srv: &str) -> RequestBuilder {
        self._request3(method, srv, (None as Option<&str>).into_iter(), |_| ())
    }

    pub fn _request2<I>(&self, method: Method, srv: &str, segments: I) -> RequestBuilder where I: IntoIterator, I::Item: AsRef<str> {
        self._request3(method, srv, segments, |_| ())
    }

    pub fn _request3<I, F>(&self, method: Method, srv: &str, segments: I, url_f: F) -> RequestBuilder
        where I: IntoIterator, I::Item: AsRef<str>, F: Fn(&mut Url) -> () {
        let mut url = Url::parse(&format!("http://{}/v1/{}", self.address, srv)).unwrap();
        url.path_segments_mut().unwrap().extend(segments);
        url_f(&mut url);

        self.client.request(method, url)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
