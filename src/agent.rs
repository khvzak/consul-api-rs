use hyper;
use hyper::method::Method::{Get, Put};
use hyper::header::ContentType;

use serde_json;
use ::JValue;

use Consul;
use error::consul_error;

use std::io::Read;
use std::collections::HashMap;

// AgentCheck represents a check known to the agent
#[derive(Deserialize, Debug, Clone)]
pub struct AgentCheck {
    #[serde(rename = "Node")]
    pub node: String,
    #[serde(rename = "CheckID")]
    pub check_id: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "Notes")]
    pub notes: String,
    #[serde(rename = "Output")]
    pub output: String,
    #[serde(rename = "ServiceID")]
    pub service_id: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
}

// AgentService represents a service known to the agent
#[derive(Deserialize, Debug, Clone)]
pub struct AgentService {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Service")]
    pub service: String,
    #[serde(rename = "Tags")]
    pub tags: Vec<String>,
    #[serde(rename = "Port")]
    pub port: u32,
    #[serde(rename = "Address")]
    pub address: String,
    #[serde(rename = "EnableTagOverride")]
    pub enable_tag_override: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct AgentCheckRegistration {
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "ServiceID", skip_serializing_if = "Option::is_none")]
    pub service_id: Option<String>,

    #[serde(rename = "Script", skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
    #[serde(rename = "DockerContainerID", skip_serializing_if = "Option::is_none")]
    pub docker_container_id: Option<String>,
    #[serde(rename = "Shell", skip_serializing_if = "Option::is_none")]
    pub shell: Option<String>,
    #[serde(rename = "Interval", skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    #[serde(rename = "Timeout", skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(rename = "TTL", skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    #[serde(rename = "HTTP", skip_serializing_if = "Option::is_none")]
    pub http: Option<String>,
    #[serde(rename = "TCP", skip_serializing_if = "Option::is_none")]
    pub tcp: Option<String>,
    #[serde(rename = "Status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename = "Notes", skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    #[serde(rename = "TLSSkipVerify", skip_serializing_if = "Option::is_none")]
    pub tls_skip_verify: Option<bool>,
    #[serde(rename = "DeregisterCriticalServiceAfter", skip_serializing_if = "Option::is_none")]
    pub deregister_critical_service_after: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct AgentServiceRegistration {
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "Name")]
    pub name: String,

    #[serde(rename = "Tags", skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(rename = "Port", skip_serializing_if = "Option::is_none")]
    pub port: Option<u32>,
    #[serde(rename = "Address", skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(rename = "EnableTagOverride", skip_serializing_if = "Option::is_none")]
    pub enable_tag_override: Option<bool>,
    #[serde(rename = "Check", skip_serializing_if = "Option::is_none")]
    pub check: Option<AgentCheckRegistration>,
}

impl From<JValue> for AgentCheckRegistration {
    fn from(x: JValue) -> Self {
        serde_json::from_value(x).unwrap()
    }
}

impl From<JValue> for AgentServiceRegistration {
    fn from(x: JValue) -> Self {
        serde_json::from_value(x).unwrap()
    }
}

pub struct Agent<'a> {
    consul: &'a Consul
}

impl<'a> Agent<'a> {
    pub fn new(consul: &'a Consul) -> Self {
        Agent { consul: consul }
    }

    pub fn checks(&self) -> ::Result<HashMap<String, AgentCheck>> {
        let mut res = self.consul._request1(Get, "agent/checks").send()?;
        match res.status {
            hyper::Ok => {
                let mut buf = String::new();
                res.read_to_string(&mut buf).expect("Cannot fill the buffer");

                let v: JValue = serde_json::from_str(&buf).expect("Cannot parse JSON");
                Ok(
                    v.as_object().unwrap().iter().map(|(k, v)| (k.clone(), serde_json::from_value(v.clone()).unwrap()))
                        .collect::<HashMap<_, _>>()
                )
            },
            _ => Err(consul_error(res)),
        }
    }

    pub fn register_check(&self, check: &AgentCheckRegistration) -> ::Result<()> {
        let res = self.consul._request1(Put, "agent/check/register")
            .body(&serde_json::to_string(check).unwrap())
            .header(ContentType::json())
            .send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    pub fn deregister_check(&self, check_id: &str) -> ::Result<()> {
        let res = self.consul._request2(Get, "agent/check/deregister", &[check_id]).send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    fn _set_check_status(&self, check_id: &str, status: &str, note: Option<&str>) -> ::Result<()> {
        let res = self.consul._request3(
            Get, "agent/check", &[status, check_id],
            |u| if let Some(note) = note { u.query_pairs_mut().append_pair("note", note); }
        ).send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    pub fn pass_check(&self, check_id: &str, note: Option<&str>) -> ::Result<()> {
        self._set_check_status(check_id, "pass", note)
    }

    pub fn warn_check(&self, check_id: &str, note: Option<&str>) -> ::Result<()> {
        self._set_check_status(check_id, "warn", note)
    }

    pub fn fail_check(&self, check_id: &str, note: Option<&str>) -> ::Result<()> {
        self._set_check_status(check_id, "fail", note)
    }

    pub fn services(&self) -> ::Result<HashMap<String, AgentService>> {
        let mut res = self.consul._request1(Get, "agent/services").send()?;
        match res.status {
            hyper::Ok => {
                let mut buf = String::new();
                res.read_to_string(&mut buf).expect("Cannot fill the buffer");

                let v: JValue = serde_json::from_str(&buf).expect("Cannot parse JSON");
                Ok(
                    v.as_object().unwrap().iter().map(|(k, v)| (k.clone(), serde_json::from_value(v.clone()).unwrap()))
                        .collect::<HashMap<_, _>>()
                )
            },
            _ => Err(consul_error(res)),
        }
    }

    pub fn register_service(&self, service: &AgentServiceRegistration) -> ::Result<()> {
        let res = self.consul._request1(Put, "agent/service/register")
            .body(&serde_json::to_string(service).unwrap())
            .header(ContentType::json())
            .send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    pub fn deregister_service(&self, service_id: &str) -> ::Result<()> {
        let res = self.consul._request2(Get, "agent/service/deregister", &[service_id]).send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    pub fn maintenance_service(&self, service_id: &str, enable: bool, reason: Option<&str>) -> ::Result<()> {
        let res = self.consul._request3(
            Put, "agent/service/maintenance", &[service_id],
            |u| if let Some(r) = reason {
                u.query_pairs_mut().append_pair("reason", r).append_pair("enable", if enable { "true" } else { "false" });
            }
        ).send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    pub fn pass_service_check(&self, service_id: &str, note: Option<&str>) -> ::Result<()> {
        self.pass_check(&format!("service:{}", service_id), note)
    }

    pub fn warn_service_check(&self, service_id: &str, note: Option<&str>) -> ::Result<()> {
        self.warn_check(&format!("service:{}", service_id), note)
    }

    pub fn fail_service_check(&self, service_id: &str, note: Option<&str>) -> ::Result<()> {
        self.fail_check(&format!("service:{}", service_id), note)
    }

    pub fn members_j(&self, wan: bool) -> ::Result<JValue> {
        let mut res = self.consul._request3(
            Get, "agent", &["members"],
            |u| if wan { u.query_pairs_mut().append_pair("wan", "1"); }
        ).send()?;
        match res.status {
            hyper::Ok => {
                let mut buf = String::new();
                res.read_to_string(&mut buf).expect("Cannot fill the buffer");

                let v: JValue = serde_json::from_str(&buf).expect("Cannot parse JSON");
                Ok(v)
            },
            _ => Err(consul_error(res)),
        }
    }

    pub fn self_j(&self) -> ::Result<JValue> {
        let mut res = self.consul._request1(Get, "agent/self").send()?;
        match res.status {
            hyper::Ok => {
                let mut buf = String::new();
                res.read_to_string(&mut buf).expect("Cannot fill the buffer");

                let v: JValue = serde_json::from_str(&buf).expect("Cannot parse JSON");
                Ok(v)
            },
            _ => Err(consul_error(res)),
        }
    }

    pub fn reload(&self) -> ::Result<()> {
        let res = self.consul._request1(Put, "agent/reload").send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    pub fn maintenance(&self, enable: bool, reason: Option<&str>) -> ::Result<()> {
        let res = self.consul._request3(
            Put, "agent", &["maintenance"],
            |u| if let Some(r) = reason {
                u.query_pairs_mut().append_pair("reason", r).append_pair("enable", if enable { "true" } else { "false" });
            }
        ).send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    pub fn join(&self, address: &str, wan: bool) -> ::Result<()> {
        let res = self.consul._request3(
            Put, "agent/join", &[address],
            |u| if wan { u.query_pairs_mut().append_pair("wan", "1"); }
        ).send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    pub fn leave(&self) -> ::Result<()> {
        let res = self.consul._request1(Put, "agent/leave").send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }

    pub fn force_leave(&self, node: &str) -> ::Result<()> {
        let res = self.consul._request2(Put, "agent/force-leave", &[node]).send()?;
        match res.status {
            hyper::Ok => Ok(()),
            _ => Err(consul_error(res)),
        }
    }
}

#[cfg(test)]
mod tests {
    use ::{Consul, AgentCheckRegistration, AgentServiceRegistration};

    #[test]
    fn checks() {
        let consul = Consul::default();

        assert!(consul.agent().register_check(&AgentCheckRegistration {
            name: "test_check".into(),
            ttl: Some("15s".into()),
            status: Some("critical".into()),
            .. Default::default()
        }).is_ok());
        assert!(consul.agent().checks().unwrap().contains_key("test_check"));

        assert!(consul.agent().pass_check("test_check", None).is_ok());
        assert!(consul.agent().checks().unwrap()["test_check"].status.as_str() == "passing");

        assert!(consul.agent().warn_check("test_check", None).is_ok());
        assert!(consul.agent().checks().unwrap()["test_check"].status.as_str() == "warning");

        assert!(consul.agent().fail_check("test_check", None).is_ok());
        assert!(consul.agent().checks().unwrap()["test_check"].status.as_str() == "critical");

        assert!(consul.agent().deregister_check("test_check").is_ok());
        assert!(consul.agent().checks().unwrap().contains_key("test_check") == false);
    }

    #[test]
    fn services() {
        let consul = Consul::default();

        assert!(consul.agent().register_service(&AgentServiceRegistration {
            name: "test_service".into(),
            tags: Some(vec!["testsrv".into()]),
            .. Default::default()
        }).is_ok());

        let services = consul.agent().services().ok().unwrap();
        assert!(services.contains_key("test_service"));
        assert!(services["test_service"].tags == vec!["testsrv".to_string()]);

        assert!(consul.agent().deregister_service("test_service").is_ok());
        assert!(consul.agent().services().ok().unwrap().contains_key("test_service") == false);
    }

    #[test]
    fn _self() {
        let consul = Consul::default();

        let conf = consul.agent().self_j();
        assert!(conf.is_ok());
        assert!(conf.unwrap().as_object().unwrap().contains_key("Config"));
    }
}
