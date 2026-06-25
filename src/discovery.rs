use crate::SERVICE_TYPE;
use gethostname::gethostname;
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
    pub fullname: String,
    pub hostname: String,
    pub addr: SocketAddr,
}

pub fn register_service(port: u16) -> anyhow::Result<ServiceDaemon> {
    let daemon = ServiceDaemon::new()?;
    let hostname = gethostname().to_string_lossy().into_owned();
    let service_name = format!("{}-{}", hostname, port);
    let hostname_fqdn = format!("{}.local.", &hostname.to_lowercase());

    let service_info =
        ServiceInfo::new(SERVICE_TYPE, &service_name, &hostname_fqdn, "", port, None)?
            .enable_addr_auto();

    daemon.register(service_info)?;

    Ok(daemon)
}

pub async fn scan_for_receivers(tx: mpsc::Sender<DiscoveryEvent>) -> anyhow::Result<()> {
    let daemon = ServiceDaemon::new()?;
    let receiver = daemon.browse(SERVICE_TYPE)?;

    let mut discovered_devices: HashMap<String, SocketAddr> = HashMap::new();

    let mut verify_interval = time::interval(Duration::from_secs(10));
    verify_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
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

                            let is_new_or_changed = match discovered_devices.get(&fullname) {
                                Some(&existing_addr) => existing_addr != socket_addr,
                                None => true,
                            };

                            if is_new_or_changed {
                                discovered_devices.insert(fullname.clone(), socket_addr);

                                let device = DiscoveredDevice {
                                    hostname: info.get_hostname().to_string(),
                                    fullname,
                                    addr: socket_addr,
                                };

                                if tx.send(DiscoveryEvent::DeviceFound(device)).await.is_err() {
                                    break;
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
    use super::*;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mdns_discovery_loopback() -> anyhow::Result<()> {
        // We use a highly specific random port to ensure we don't accidentally
        // test against a real running instance of your app!
        let test_port = 54321;

        // 1. Start the Broadcaster
        let _broadcaster_daemon = register_service(test_port)?;

        // Give the OS a tiny moment to register the UDP socket and fire the packet
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 2. Setup the Scanner channel
        let (tx, mut rx) = mpsc::channel(5);

        // 3. Spawn the Scanner in a background task
        let scanner_task = tokio::spawn(async move {
            let _ = scan_for_receivers(tx).await;
        });

        // 4. Await the discovery with a strict timeout so a failing test doesn't hang forever
        let start = std::time::Instant::now();
        let mut found = false;
        while start.elapsed() < Duration::from_secs(3) {
            if let Ok(Some(DiscoveryEvent::DeviceFound(device))) =
                tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
            {
                println!("Found device: {} at {}", device.hostname, device.addr);
                if device.addr.port() == test_port {
                    found = true;
                    break;
                }
            } else if rx.is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        // 5. Verification
        if !found {
            anyhow::bail!("Did not discover the test device on port {}", test_port);
        }

        // 6. Cleanup
        // Abort the scanner task, which drops the channel and shuts down the mdns daemon
        scanner_task.abort();

        Ok(())
    }
}
