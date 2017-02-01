use hyper::{self, Url};
use hyper::header::ContentType;

use serde_json::Value as JValue;

use Consul;
use error::consul_error;

pub struct Agent<'a> {
    consul: &'a Consul
}

impl<'a> Agent<'a> {
    pub fn new(consul: &'a Consul) -> Self {
        Agent { consul: consul }
    }

    pub fn register_service_j(&self, service_def: JValue) -> ::Result<()> {
        assert!(service_def.is_object());

        let client = &self.consul.client;
        let res = client.put(&format!("http://{}/v1/agent/service/register", self.consul.address))
            .body(&service_def.to_string())
            .header(ContentType::json())
            .send()?;

        if res.status != hyper::Ok {
            return Err(consul_error(res));
        }

        Ok(())
    }

    fn _set_service_check(&self, x: &str, service_id: &str, note: Option<&str>) -> ::Result<()> {
        let client = &self.consul.client;
        let mut url = Url::parse(&format!("http://{}/v1/agent/check/{}/service:{}", self.consul.address, x, service_id))?;
        if let Some(note) = note {
            url.query_pairs_mut().append_pair("note", note);
        }

        let res = client.get(url).send()?;

        if res.status != hyper::Ok {
            return Err(consul_error(res));
        }

        Ok(())
    }

    pub fn pass_service_check(&self, service_id: &str, note: Option<&str>) -> ::Result<()> {
        self._set_service_check("pass", service_id, note)
    }

    pub fn warn_service_check(&self, service_id: &str, note: Option<&str>) -> ::Result<()> {
        self._set_service_check("warn", service_id, note)
    }

    pub fn fail_service_check(&self, service_id: &str, note: Option<&str>) -> ::Result<()> {
        self._set_service_check("fail", service_id, note)
    }
}
