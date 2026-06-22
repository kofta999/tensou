use crate::SERVICE_TYPE;
use gethostname::gethostname;
use mdns_sd::{ScopedIp, ServiceDaemon, ServiceEvent, ServiceInfo};
use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
};
use tokio::sync::mpsc;

pub struct DiscoveredDevice {
    pub hostname: String,
    pub addr: SocketAddr,
}

pub fn register_service(port: u16) -> anyhow::Result<ServiceDaemon> {
    let daemon = ServiceDaemon::new()?;
    let hostname = gethostname().to_string_lossy().into_owned();
    let service_name = &hostname;
    let hostname = format!("{}.local.", &hostname.to_lowercase());

    let service_info =
        ServiceInfo::new(SERVICE_TYPE, service_name, &hostname, "", port, None)?.enable_addr_auto();

    daemon.register(service_info)?;

    Ok(daemon)
}

pub async fn scan_for_receivers(tx: mpsc::Sender<DiscoveredDevice>) -> anyhow::Result<()> {
    let daemon = ServiceDaemon::new()?;
    let receiver = daemon.browse(SERVICE_TYPE)?;

    let mut discovered = HashSet::new();

    println!("Scanning local network devices...");

    while let Ok(event) = receiver.recv_async().await {
        if let ServiceEvent::ServiceResolved(info) = event {
            if let Some(ScopedIp::V4(addr)) = info.get_addresses().iter().find(|ip| ip.is_ipv4()) {
                let socket_addr = SocketAddr::new(IpAddr::V4(*addr.addr()), info.port);

                if discovered.insert(socket_addr) {
                    let device = DiscoveredDevice {
                        hostname: info.get_hostname().to_string(),
                        addr: socket_addr,
                    };

                    if tx.send(device).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    println!("Scan complete.");
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
        let discovery_result = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;

        // 5. Verification
        match discovery_result {
            Ok(Some(device)) => {
                println!("Found test device: {} at {}", device.hostname, device.addr);

                // Assert we found OUR test instance, not something else on the network
                assert_eq!(device.addr.port(), test_port);
            }
            Ok(None) => anyhow::bail!("Channel closed without finding a device"),
            Err(_) => anyhow::bail!(
                "mDNS scan timed out. Your OS might be dropping loopback multicast packets."
            ),
        }

        // 6. Cleanup
        // Abort the scanner task, which drops the channel and shuts down the mdns daemon
        scanner_task.abort();

        Ok(())
    }
}
