use hyper;
use hyper::method::Method::{Get, Put};
use hyper::header::ContentType;

use serde_json::{self, Value as JValue, Map as JMap};

use Consul;
use error::consul_error;

use std::io::Read;
use std::collections::HashMap;

pub struct Agent<'a> {
    consul: &'a Consul
}

pub trait HasJValue {
    fn _jv(&self) -> &JValue;
    fn _jv_mut(&mut self) -> &mut JValue;
}

// AgentCheckTrait represents a check known to the agent
pub trait AgentCheckTrait : HasJValue {
    fn node(&self)          -> &str { self._jv().get("Node").and_then(|x| x.as_str()).unwrap() }
    fn check_id(&self)      -> &str { self._jv().get("CheckID").and_then(|x| x.as_str()).unwrap() }
    fn name(&self)          -> &str { self._jv().get("Name").and_then(|x| x.as_str()).unwrap() }
    fn status(&self)        -> &str { self._jv().get("Status").and_then(|x| x.as_str()).unwrap() }
    fn notes(&self)         -> &str { self._jv().get("Notes").and_then(|x| x.as_str()).unwrap() }
    fn output(&self)        -> &str { self._jv().get("Output").and_then(|x| x.as_str()).unwrap() }
    fn service_id(&self)    -> &str { self._jv().get("ServiceID").and_then(|x| x.as_str()).unwrap() }
    fn service_name(&self)  -> &str { self._jv().get("ServiceName").and_then(|x| x.as_str()).unwrap() }
}

pub trait AgentCheckRegistrationTrait : AgentServiceCheckTrait {
    fn id(&self) -> Option<&str> {
        self._jv().get("ID").and_then(|x| x.as_str())
    }

    fn set_id<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("ID".into(), JValue::String(val.into()));
        self
    }

    fn name(&self) -> Option<&str> {
        self._jv().get("Name").and_then(|x| x.as_str())
    }

    fn set_name<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Name".into(), JValue::String(val.into()));
        self
    }

    fn notes(&self) -> Option<&str> {
        self._jv().get("Notes").and_then(|x| x.as_str())
    }

    fn set_notes<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Notes".into(), JValue::String(val.into()));
        self
    }

    fn service_id(&self) -> Option<&str> {
        self._jv().get("ServiceID").and_then(|x| x.as_str())
    }

    fn set_service_id<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("ServiceID".into(), JValue::String(val.into()));
        self
    }
}

// AgentServiceTrait represents a service known to the agent
pub trait AgentServiceTrait : HasJValue {
    fn id(&self)                    -> &str { self._jv().get("ID").and_then(|x| x.as_str()).unwrap() }
    fn service(&self)               -> &str { self._jv().get("Service").and_then(|x| x.as_str()).unwrap() }
    fn tags(&self)                  -> Vec<&str> {
        self._jv().get("Tags")
            .and_then(|x| x.as_array())
            .map(|vec| vec.into_iter().map(|x| x.as_str().unwrap()).collect::<Vec<_>>())
            .unwrap()
    }
    fn port(&self)                  -> u32 { self._jv().get("CheckID").and_then(|x| x.as_u64()).map(|x| x as u32).unwrap() }
    fn address(&self)               -> &str { self._jv().get("Address").and_then(|x| x.as_str()).unwrap() }
    fn enable_tag_override(&self)   -> bool { self._jv().get("EnableTagOverride").and_then(|x| x.as_bool()).unwrap() }
}

// AgentServiceRegistrationTrait is used to register a new service
pub trait AgentServiceRegistrationTrait : HasJValue {
    fn id(&self) -> Option<&str> {
        self._jv().get("ID").and_then(|x| x.as_str())
    }

    fn set_id<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("ID".into(), JValue::String(val.into()));
        self
    }

    fn name(&self) -> Option<&str> {
        self._jv().get("Name").and_then(|x| x.as_str())

    }

    fn set_name<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Name".into(), JValue::String(val.into()));
        self
    }

    fn tags(&self) -> Option<Vec<&str>> {
        self._jv().get("Tags")
            .and_then(|x| x.as_array())
            .map(|vec| vec.into_iter().map(|x| x.as_str().unwrap()).collect::<Vec<_>>())
    }

    fn set_tags(&mut self, val: &[&str]) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert(
            "Tags".into(),
            JValue::Array( val.iter().map(|&x| JValue::String(x.into())).collect() )
        );
        self
    }

    fn port(&self) -> Option<u32> {
        self._jv().get("Port").and_then(|x| x.as_u64()).map(|x| x as u32)
    }

    fn set_port(&mut self, val: u32) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Port".into(), val.into());
        self
    }

    fn address(&self) -> Option<&str> {
        self._jv().get("Address").and_then(|x| x.as_str())

    }

    fn set_address<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Address".into(), JValue::String(val.into()));
        self
    }

    fn enable_tag_override(&self) -> Option<bool> {
        self._jv().get("EnableTagOverride").and_then(|x| x.as_bool())
    }

    fn set_enable_tag_override(&mut self, val: bool) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("EnableTagOverride".into(), JValue::Bool(val));
        self
    }

    fn check(&self) -> Option<AgentServiceCheck> {
        self._jv().get("Check").map(|x| AgentServiceCheck(x.clone()))
    }

    fn set_check(&mut self, val: AgentServiceCheck) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Check".into(), val.0);
        self
    }
}

pub trait AgentServiceCheckTrait : HasJValue {
    fn script(&self)  -> Option<&str> {
        self._jv().get("Script").and_then(|x| x.as_str())
    }

    fn set_script<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Script".into(), JValue::String(val.into()));
        self
    }

    fn docker_container_id(&self) -> Option<&str> {
        self._jv().get("DockerContainerID").and_then(|x| x.as_str())
    }

    fn set_docker_container_id<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("DockerContainerID".into(), JValue::String(val.into()));
        self
    }

    fn shell(&self) -> Option<&str> {
        self._jv().get("Shell").and_then(|x| x.as_str())
    }

    fn set_shell<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Shell".into(), JValue::String(val.into()));
        self
    }

    fn interval(&self) -> Option<&str> {
        self._jv().get("Interval").and_then(|x| x.as_str())
    }

    fn set_interval<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Interval".into(), JValue::String(val.into()));
        self
    }

    fn timeout(&self) -> Option<&str> {
        self._jv().get("Timeout").and_then(|x| x.as_str())
    }

    fn set_timeout<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Timeout".into(), JValue::String(val.into()));
        self
    }

    fn ttl(&self) -> Option<&str> {
        self._jv().get("TTL").and_then(|x| x.as_str())
    }

    fn set_ttl<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("TTL".into(), JValue::String(val.into()));
        self
    }

    fn http(&self) -> Option<&str> {
        self._jv().get("HTTP").and_then(|x| x.as_str())
    }

    fn set_http<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("HTTP".into(), JValue::String(val.into()));
        self
    }

    fn tcp(&self) -> Option<&str> {
        self._jv().get("TCP").and_then(|x| x.as_str())
    }

    fn set_tcp<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("TCP".into(), JValue::String(val.into()));
        self
    }

    fn status(&self) -> Option<&str> {
        self._jv().get("Status").and_then(|x| x.as_str())
    }

    fn set_status<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Status".into(), JValue::String(val.into()));
        self
    }

    fn notes(&self) -> Option<&str> {
        self._jv().get("Notes").and_then(|x| x.as_str())
    }

    fn set_notes<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("Notes".into(), JValue::String(val.into()));
        self
    }

    fn tls_skip_verify(&self) -> Option<&str> {
        self._jv().get("TLSSkipVerify").and_then(|x| x.as_str())
    }

    fn set_tls_skip_verify<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("TLSSkipVerify".into(), JValue::String(val.into()));
        self
    }

    fn deregister_critical_service_after(&self) -> Option<&str> {
        self._jv().get("DeregisterCriticalServiceAfter").and_then(|x| x.as_str())
    }

    fn set_deregister_critical_service_after<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self._jv_mut().as_object_mut().unwrap().insert("DeregisterCriticalServiceAfter".into(), JValue::String(val.into()));
        self
    }
}

#[derive (Clone, Debug)]
pub struct AgentCheck(JValue);

#[derive (Clone, Debug)]
pub struct AgentCheckRegistration(JValue);

impl HasJValue for AgentCheck {
    fn _jv(&self) -> &JValue { &self.0 }
    fn _jv_mut(&mut self) -> &mut JValue { &mut self.0 }
}

impl AgentCheckTrait for AgentCheck {}

impl HasJValue for AgentCheckRegistration {
    fn _jv(&self) -> &JValue { &self.0 }
    fn _jv_mut(&mut self) -> &mut JValue { &mut self.0 }
}

impl AgentCheckRegistrationTrait for AgentCheckRegistration {}
impl AgentServiceCheckTrait for AgentCheckRegistration {}

impl AgentCheckRegistration {
    pub fn new(name: &str) -> Self {
        let mut s = AgentCheckRegistration(JValue::Object(JMap::new()));
        s.set_name(name);
        s
    }
}

#[derive (Clone, Debug)]
pub struct AgentService(JValue);

#[derive (Clone, Debug)]
pub struct AgentServiceCheck(JValue);

#[derive (Clone, Debug)]
pub struct AgentServiceRegistration(JValue);

impl HasJValue for AgentService {
    fn _jv(&self) -> &JValue { &self.0 }
    fn _jv_mut(&mut self) -> &mut JValue { &mut self.0 }
}

impl AgentServiceTrait for AgentService {}

impl HasJValue for AgentServiceCheck {
    fn _jv(&self) -> &JValue { &self.0 }
    fn _jv_mut(&mut self) -> &mut JValue { &mut self.0 }
}

impl AgentServiceCheckTrait for AgentServiceCheck {}

impl HasJValue for AgentServiceRegistration {
    fn _jv(&self) -> &JValue { &self.0 }
    fn _jv_mut(&mut self) -> &mut JValue { &mut self.0 }
}

impl AgentServiceRegistrationTrait for AgentServiceRegistration {}

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
                Ok(v.as_object().unwrap().iter().map(|(k, v)| (k.clone(),AgentCheck(v.clone()))).collect::<HashMap<_, _>>())
            },
            _ => Err(consul_error(res)),
        }
    }

    pub fn register_check(&self, check: AgentCheckRegistration) -> ::Result<()> {
        let res = self.consul._request1(Put, "agent/checks/register")
            .body(check.0.as_str().unwrap())
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
                Ok(v.as_object().unwrap().iter().map(|(k, v)| (k.clone(),AgentService(v.clone()))).collect::<HashMap<_, _>>())
            },
            _ => Err(consul_error(res)),
        }
    }

    pub fn register_service(&self, service: AgentServiceRegistration) -> ::Result<()> {
        let res = self.consul._request1(Put, "agent/service/register")
            .body(service.0.as_str().unwrap())
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
