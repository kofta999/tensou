mod connection_manager;
mod recv;
mod send;
pub use recv::Receiver;
pub use recv::ReceiverDaemon;
pub use send::SendType;
pub use send::Sender;

use crate::protocol::TransferError;

pub fn is_connection_error(e: &anyhow::Error) -> bool {
    for cause in e.chain() {
        if let Some(we) = cause.downcast_ref::<quinn::WriteError>()
            && let quinn::WriteError::ConnectionLost(ce) = we
            && is_quinn_connection_error(ce)
        {
            return true;
        }
        if let Some(ce) = cause.downcast_ref::<quinn::ConnectionError>()
            && is_quinn_connection_error(ce)
        {
            return true;
        }
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>()
            && (io_err.kind() == std::io::ErrorKind::ConnectionReset
                || io_err.kind() == std::io::ErrorKind::ConnectionAborted
                || io_err.kind() == std::io::ErrorKind::NotConnected
                || io_err.kind() == std::io::ErrorKind::BrokenPipe)
        {
            return true;
        }
    }
    false
}

fn is_quinn_connection_error(e: &quinn::ConnectionError) -> bool {
    match e {
        quinn::ConnectionError::TimedOut
        | quinn::ConnectionError::Reset
        | quinn::ConnectionError::TransportError(_)
        | quinn::ConnectionError::LocallyClosed
        | quinn::ConnectionError::ConnectionClosed(_) => true,
        quinn::ConnectionError::ApplicationClosed(app_close) => {
            let code = u64::from(app_close.error_code) as u32;
            let error = TransferError::from_code(code).unwrap_or(TransferError::ConnectionLoss);
            !matches!(error, TransferError::Cancelled | TransferError::Rejected)
        }
        _ => false,
    }
}
