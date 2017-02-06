use hyper;
use hyper::method::Method::{Get, Put};
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

    //pub fn checks_j(&self) -> ::Result<JValue> {
    //    let res = self.consul.client.get()
    //}

    pub fn register_service_j(&self, service_def: JValue) -> ::Result<()> {
        assert!(service_def.is_object());

        let res = self.consul._request(Put, "agent/service/register")
            .body(&service_def.to_string())
            .header(ContentType::json())
            .send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    fn _set_service_check(&self, service_id: &str, status: &str, note: Option<&str>) -> ::Result<()> {
        let res = self.consul._request3(Get,
                                        "agent/check",
                                        &[status, &format!("service:{}", service_id)],
                                        |u| if let Some(note) = note { u.query_pairs_mut().append_pair("note", note); }).send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
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
