use hyper;
use hyper::method::Method::{Get, Put, Delete};
//use hyper::header::ContentType;

use base64;
use serde;
use serde::de::{Deserialize, Deserializer};
use serde_json;
use ::JValue;

use Consul;
use error::consul_error;

use std::io::Read;

pub struct KeyValue<'a> {
    consul: &'a Consul
}

#[derive(Clone, Debug, Deserialize)]
struct KVEntry {
    #[serde(rename = "CreateIndex")]
    create_index: u64,
    #[serde(rename = "ModifyIndex")]
    modify_index: u64,
    #[serde(rename = "LockIndex")]
    lock_index: u64,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Flags")]
    flags: u64,
    #[serde(rename = "Value", deserialize_with = "_deserialize_base64")]
    value: Vec<u8>,
    #[serde(default, rename = "Session")]
    session: Option<String>,
}

fn _deserialize_base64<D>(deserializer: D) -> Result<Vec<u8>, D::Error> where D: Deserializer {
    let s = try!(String::deserialize(deserializer));
    base64::decode(&s).map_err(serde::de::Error::custom)
}

/*
pub struct KVTransaction<'a> {
    consul: &'a Consul,
    ops: Vec<JValue>,
}
*/

impl<'a> KeyValue<'a> {
    pub fn new(consul: &'a Consul) -> Self {
        KeyValue { consul: consul }
    }

    pub fn get(&self, key: &str) -> ::Result<Option<String>> {
        match self.get_entry(key) {
            Err(err) => Err(err),
            Ok(None) => Ok(None),
            Ok(Some(kv)) => String::from_utf8(kv.value).map(|v| Some(v)).map_err(|e| ::std::convert::From::from(e)),
        }
    }

    pub fn get_bytes(&self, key: &str) -> ::Result<Option<Vec<u8>>> {
        self.get_entry(key).map(|x| x.map(|kv| kv.value))
    }

    fn get_entry(&self, key: &str) -> ::Result<Option<KVEntry>> {
        let mut res = self.consul._request2(Get, "kv", key.split('/')).send()?;
        match res.status {
            hyper::NotFound => Ok(None),
            hyper::Ok => {
                let mut buf = String::new();
                res.read_to_string(&mut buf).expect("Cannot fill the buffer");

                let mut entry: Vec<KVEntry> = serde_json::from_str(&buf).expect("Cannot parse JSON");
                assert!(entry.len() == 1);
                Ok(Some(entry.pop().unwrap()))
            },
            _ => Err(consul_error(res))
        }
    }

    pub fn contains_key(&self, key: &str) -> ::Result<bool> {
        let res = self.consul._request2(Get, "kv", key.split('/')).send()?;
        match res.status {
            hyper::NotFound => Ok(false),
            hyper::Ok => Ok(true),
            _ => Err(consul_error(res)),
        }
    }

    pub fn keys(&self, prefix: &str) -> ::Result<Vec<String>> {
        let mut res = self.consul._request3(Get, "kv", prefix.split('/'), |u| u.set_query(Some("keys"))).send()?;
        match res.status {
            hyper::Ok => {
                let mut buf = String::new();
                res.read_to_string(&mut buf).expect("Cannot fill the buffer");

                let v: JValue = serde_json::from_str(&buf).expect("Cannot parse JSON");
                Ok(v.as_array().unwrap().into_iter().map(|x| x.as_str().unwrap().to_string()).collect::<Vec<_>>())
            },
            _ => Err(consul_error(res)),
        }
    }

    pub fn insert(&self, key: &str, value: &str) -> ::Result<bool> {
        self.insert_bytes(key, value.as_bytes())
    }

    pub fn insert_bytes(&self, key: &str, value: &[u8]) -> ::Result<bool> {
        let mut res = self.consul._request2(Put, "kv", key.split('/')).body(value).send()?;
        match res.status {
            hyper::Ok => {
                let mut buf = String::new();
                res.read_to_string(&mut buf).expect("Cannot fill the buffer");
                Ok(buf.trim_right() == "true")
            },
            _ => Err(consul_error(res)),
        }
    }

    pub fn remove(&self, key: &str) -> ::Result<()> {
        let res = self.consul._request2(Delete, "kv", key.split('/')).send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    pub fn remove_tree(&self, prefix: &str) -> ::Result<()> {
        let res = self.consul._request3(Delete, "kv", prefix.split('/'), |u| u.set_query(Some("recurse"))).send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    /*
    pub fn begin_transaction(&self) -> KVTransaction {
        KVTransaction { consul: self.consul, ops: Vec::new() }
    }
    */
}

#[cfg(test)]
mod tests {
    use ::Consul;

    #[test]
    fn base_ops() {
        let consul = Consul::default();

        assert!(consul.kv().insert("test/key0", "hello world").unwrap());
        assert!(consul.kv().insert("test/key1", "my • test • value").unwrap());
        assert!(consul.kv().insert("test/key2/val", "level3 val").unwrap());

        assert!(consul.kv().contains_key("test/key0").unwrap());
        assert!(!consul.kv().contains_key("test/none").unwrap());

        assert_eq!(consul.kv().get("test/key0").unwrap(), Some("hello world".to_string()));
        assert_eq!(consul.kv().get("test/none").unwrap(), None);

        assert_eq!(consul.kv().keys("test/").unwrap(), vec!["test/key0".to_string(), "test/key1".to_string(), "test/key2/val".to_string()]);

        assert!(consul.kv().remove("test/key0").is_ok());
        assert_eq!(consul.kv().get("test/key0").unwrap(), None);

        assert!(consul.kv().remove_tree("test/").is_ok());
        assert!(consul.kv().keys("test/").is_err());
    }
}
