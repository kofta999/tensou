use crate::{SERVICE_TYPE, config::Config};
use mdns_sd::{ScopedIp, ServiceDaemon, ServiceEvent, ServiceInfo};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use tokio::{sync::mpsc, time};

#[derive(Debug)]
pub enum DiscoveryEvent {
    DeviceFound(DiscoveredDevice),
    DeviceLost(String),
}

#[derive(Debug)]
pub struct DiscoveredDevice {
    pub display_name: String,
    pub device_uuid: String,
    pub os_type: String,
    pub addr: SocketAddr,
}

pub fn register_service(config: &Config) -> anyhow::Result<ServiceDaemon> {
    let daemon = ServiceDaemon::new()?;
    let hostname = config.display_name.clone();
    let instance_name = config.display_name.clone();
    let hostname_fqdn = format!("{}.local.", &hostname.to_lowercase());

    let mut props = HashMap::new();

    props.insert("device_uuid".to_string(), config.device_uuid.clone());
    props.insert("display_name".to_string(), config.display_name.clone());
    props.insert("os_type".to_string(), config.os_type.clone());

    let service_info = ServiceInfo::new(
        SERVICE_TYPE,
        &instance_name,
        &hostname_fqdn,
        "",
        config.listen_port,
        props,
    )?
    .enable_addr_auto();

    daemon.register(service_info)?;

    Ok(daemon)
}

pub async fn scan_for_receivers(
    tx: mpsc::Sender<DiscoveryEvent>,
    _my_uuid: &str,
) -> anyhow::Result<()> {
    let daemon = ServiceDaemon::new()?;
    let receiver = daemon.browse(SERVICE_TYPE)?;

    let mut discovered_devices: HashMap<String, SocketAddr> = HashMap::new();

    let mut verify_interval = time::interval(Duration::from_secs(10));
    verify_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Remove offline devices every 3s
            _ = verify_interval.tick() => {
                for fullname in discovered_devices.keys() {
                    let _ = daemon.verify(fullname.clone(), Duration::from_secs(3));
                }
            }

            Ok(event) = receiver.recv_async() => {
                match event {
                    ServiceEvent::ServiceResolved(info) => {
                        if let Some(ScopedIp::V4(addr)) = info.get_addresses().iter().find(|ip| ip.is_ipv4()) {
                            let socket_addr = SocketAddr::new(IpAddr::V4(*addr.addr()), info.port);
                            let fullname = info.get_fullname().to_string();

                            let device_uuid = info.get_properties().get_property_val_str("device_uuid").unwrap_or_default().to_string();
                            let display_name = info.get_properties().get_property_val_str("display_name").unwrap_or_default().to_string();
                            let os_type = info.get_properties().get_property_val_str("os_type").unwrap_or_default().to_string();

                            let is_new_or_changed = match discovered_devices.get(&fullname) {
                                Some(&existing_addr) => existing_addr != socket_addr,
                                None => true,
                            };

                            if is_new_or_changed {
                                let is_valid = {
                                    #[cfg(not(debug_assertions))]
                                    {
                                        device_uuid != _my_uuid
                                    }
                                    #[cfg(debug_assertions)]
                                    {
                                        true
                                    }
                                };

                                if is_valid {
                                    discovered_devices.insert(fullname.clone(), socket_addr);

                                    let device = DiscoveredDevice {
                                        display_name,
                                        device_uuid,
                                        os_type,
                                        addr: socket_addr,
                                    };

                                    if tx.send(DiscoveryEvent::DeviceFound(device)).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    ServiceEvent::ServiceRemoved(_, fullname) => {
                        if discovered_devices.remove(&fullname).is_some() {
                            if tx.send(DiscoveryEvent::DeviceLost(fullname)).await.is_err() {
                                break;
                            }
                        }
                    }
                    _ => (),
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::config;

    use super::*;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mdns_discovery_loopback() -> anyhow::Result<()> {
        let test_port = 54321;
        let test_uuid = uuid::Uuid::new_v4().to_string(); // unique per test run

        let mut config = config::Config::default();
        config.listen_port = test_port;
        config.device_uuid = test_uuid.clone(); // register with known UUID
        let _broadcaster_daemon = register_service(&config)?;

        tokio::time::sleep(Duration::from_millis(1000)).await;

        let (tx, mut rx) = mpsc::channel(5);
        let scanner_task = tokio::spawn(async move {
            let _ = scan_for_receivers(tx, "me").await;
        });

        let start = std::time::Instant::now();
        let mut found = false;
        while start.elapsed() < Duration::from_secs(3) {
            if let Ok(Some(DiscoveryEvent::DeviceFound(device))) =
                tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
            {
                // Filter by UUID, not just port — immune to real running instances
                if device.device_uuid == test_uuid {
                    found = true;
                    break;
                }
            }
        }

        scanner_task.abort();

        if !found {
            anyhow::bail!("Did not discover the test device");
        }
        Ok(())
    }
}
