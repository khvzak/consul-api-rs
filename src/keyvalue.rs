use hyper;
//use hyper::header::ContentType;

use std::io::Read;

use base64;
use serde_json;
use serde_json::Value as JValue;

use Consul;
use error::consul_error;

pub struct KeyValue<'a> {
    consul: &'a Consul
}

impl<'a> KeyValue<'a> {
    pub fn new(consul: &'a Consul) -> Self {
        KeyValue { consul: consul }
    }

    pub fn get(&self, key: &str) -> ::Result<Option<String>> {
        match self.get_bytes(key) {
            Err(err) => Err(err),
            Ok(None) => Ok(None),
            Ok(Some(val)) => String::from_utf8(val).map(|v| Some(v)).map_err(|e| ::std::convert::From::from(e)),
        }
    }

    pub fn get_bytes(&self, key: &str) -> ::Result<Option<Vec<u8>>> {
        let client = &self.consul.client;
        let mut res = client.get(&format!("http://{}/v1/kv/{}", self.consul.address, key)).send()?;

        match res.status {
            hyper::NotFound => Ok(None),
            hyper::Ok => {
                let mut buf = String::new();
                res.read_to_string(&mut buf).expect("Cannot fill the buffer");

                let v: JValue = serde_json::from_str(&buf).expect("Cannot parse JSON");
                Ok(Some(base64::decode(v[0]["Value"].as_str().expect("Invalid JSON object")).expect("Cannot decode base64 key value")))
            },
            _ => Err(consul_error(res))
        }
    }

    pub fn insert(&self, key: &str, value: &str) -> ::Result<()> {
        self.insert_bytes(key, value.as_bytes())
    }

    pub fn insert_bytes(&self, key: &str, value: &[u8]) -> ::Result<()> {
        let client = &self.consul.client;
        let res = client.put(&format!("http://{}/v1/kv/{}", self.consul.address, key)).body(value).send()?;

        if res.status != hyper::Ok {
            return Err(consul_error(res));
        }

        Ok(())
    }
}
