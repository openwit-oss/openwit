use crate::messages::{ResourceReportMessage, ScaleCommand};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::debug;

/// The DecisionActor checks for threshold breaches and sends scale commands.
pub async fn start_decision_actor(
    mut alert_rx: Receiver<ResourceReportMessage>,
    scale_tx: Sender<ScaleCommand>,
) {
    while let Some(alert) = alert_rx.recv().await {
        debug!(
            "DecisionActor: Received alert: resource={:?}, usage={:?}%",
            alert.resource, alert.usage_percent
        );

        // Here you can add fancier logic later (e.g. debounce, resource-aware rules)
        let command = ScaleCommand {
            reason: format!(
                "{:?} usage {:?}% > threshold {:?}%",
                alert.resource, alert.usage_percent, alert.threshold_percent
            ),
            resource: alert.resource.clone(),
            usage_percent: alert.usage_percent,
        };

        if let Err(err) = scale_tx.send(command).await {
            tracing::error!("Failed to send ScaleCommand: {:?}", err);
        }
    }
}
