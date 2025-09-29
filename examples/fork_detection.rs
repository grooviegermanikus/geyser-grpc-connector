use clap::Parser;
use log::{info, warn};
use solana_sdk::commitment_config::{CommitmentConfig};
use yellowstone_grpc_proto::geyser::{SubscribeUpdateSlot, SlotStatus as ySS};
use std::collections::{HashMap, HashSet};
use std::env;
use std::time::Duration;
use anyhow::{anyhow};
use solana_sdk::clock::Slot;
use tokio::sync::broadcast;
use tonic::transport::ClientTlsConfig;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{
    map_commitment_level, GrpcConnectionTimeouts, GrpcSourceConfig, Message,
};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterSlots};
use agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use tokio::time::Instant;
// 2025-09-09T16:12:30.236552Z  INFO fork_detection: Fork-Detection: Slot 365713185 finalized, parent=365713184
// 2025-09-09T16:12:30.661459Z  INFO fork_detection: Fork-Detection: Slot 365713186 finalized, parent=365713185
// 2025-09-09T16:12:30.661600Z  INFO fork_detection: Fork-Detection: Slot 365713187 finalized, parent=365713186
// 2025-09-09T16:12:30.661642Z  WARN fork_detection: Fork-Detection: Slot 365713192 finalized, parent=365713187
// 2025-09-09T16:12:30.661669Z  INFO fork_detection: checking slot 365713188, orphan=true
// 2025-09-09T16:12:30.661692Z  INFO fork_detection: checking slot 365713189, orphan=true
// 2025-09-09T16:12:30.661714Z  INFO fork_detection: checking slot 365713190, orphan=false
// 2025-09-09T16:12:30.661736Z  INFO fork_detection: checking slot 365713191, orphan=false
// 2025-09-09T16:12:31.170278Z  INFO fork_detection: Fork-Detection: Slot 365713193 finalized, parent=365713192
// 2025-09-09T16:12:31.484739Z  INFO fork_detection: Fork-Detection: Slot 365713194 finalized, parent=365713193
// 2025-09-09T16:12:31.986628Z  INFO fork_detection: Fork-Detection: Slot 365713195 finalized, parent=365713194
// 2025-09-09T16:12:32.282026Z  INFO fork_detection: Fork-Detection: Slot 365713196 finalized, parent=365713195
// 2025-09-09T16:12:32.754367Z  INFO fork_detection: Fork-Detection: Slot 365713197 finalized, parent=365713196

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {


}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let _args = Args::parse();

    let grpc_addr = env::var("GRPC_ADDR").expect("need grpc url");
    let grpc_x_token = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source {} ({})",
        grpc_addr,
        grpc_x_token.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let tls_config = ClientTlsConfig::new().with_native_roots();

    let grpc_config =
        GrpcSourceConfig::new(grpc_addr, grpc_x_token, Some(tls_config.clone()), timeouts.clone());

    let (autoconnect_tx, mut slots_rx) = tokio::sync::mpsc::channel(10);

    let (_exit, exit_notify) = broadcast::channel(1);

    let _ah = create_geyser_autoconnection_task_with_mpsc(
        grpc_config.clone(),
        build_slot_subscription(),
        autoconnect_tx.clone(),
        exit_notify.resubscribe(),
    );

    let mut processed_slot_times: HashMap<Slot, Instant> = HashMap::with_capacity(1024);

    // after-the-fact first shred timing
    let mut first_shred_times: HashMap<Slot, Instant> = HashMap::with_capacity(1024);

    let mut current_slot: Slot = 0;

    'recv_loop: loop {
        match slots_rx.recv().await {
            Some(Message::GeyserSubscribeUpdate(update)) => match update.update_oneof {
                Some(UpdateOneof::Slot(update_msg)) => {
                    let now = Instant::now();

                    let Ok(slot_status) = map_slot_status(&update_msg) else {
                        // warn!("Fork-Detection: Slot {} invalid status {}", update_msg.slot, update_msg.status);
                        continue 'recv_loop;
                    };

                    if slot_status == SlotStatus::FirstShredReceived {
                        info!("Fork-Detection: Slot {} first-shred", update_msg.slot);
                        current_slot = update_msg.slot;
                        let inserted = first_shred_times.insert(update_msg.slot, Instant::now());
                        assert!(inserted.is_none(), "slot already had first-shred time");
                    }

                    if slot_status == SlotStatus::Processed {
                        let inserted = processed_slot_times.insert(update_msg.slot, Instant::now());
                        assert!(inserted.is_none(), "slot already had processed time");
                        info!("Fork-Detection: Slot {} processed", update_msg.slot);
                    }

                    if slot_status == SlotStatus::Confirmed {
                        info!("Fork-Detection: Slot {} confirmed", update_msg.slot);

                        // seeing "confirmed for slot N" means that no tx for N will be accepted. the current Slot which is N+1 (or higher) might take txs.
                        // note: confirmed slot does not say anything about the current slot
                        let estimated_leader_slot = update_msg.slot + 1;
                        info!("Leader schedule: Send tx to leader for slot {} with current slot {}", estimated_leader_slot, current_slot);

                    }

                    if slot_status == SlotStatus::Rooted {

                        let last_finalized_slot = update_msg.parent.expect("parent slot");
                        let diff = update_msg.slot - last_finalized_slot;
                        if diff == 1 {
                            info!("Fork-Detection: Slot {} finalized, parent={}", update_msg.slot, last_finalized_slot);
                        } else if diff > 1 {
                            warn!("Fork-Detection: Slot {} finalized, parent={}", update_msg.slot, last_finalized_slot);
                        }

                        for checking_slot in (last_finalized_slot+1)..=(update_msg.slot-1) {
                            let orphan_slot_seen = processed_slot_times.contains_key(&checking_slot);
                            info!("checking slot {}, orphan={}", checking_slot, orphan_slot_seen);
                        }
                    }

                }
                Some(_) => {}
                None => {}
            },
            None => {
                log::warn!("multiplexer channel closed - aborting");
                return;
            }
            Some(Message::Connecting(_)) => {}
        }
    }
}


fn build_slot_subscription() -> SubscribeRequest {
    let slots_sub = HashMap::from([(
        "geyser_tracker_slots_all_levels".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            // do not send "new" slot status like FirstShredReceived but only Processed, Confirmed, Finalized
            interslot_updates: Some(true),
        },
    )]);

    SubscribeRequest {
        slots: slots_sub,
        commitment: Some(map_commitment_level(CommitmentConfig::processed()) as i32),
        ..Default::default()
    }
}

fn map_slot_status(
    slot_update: &SubscribeUpdateSlot,
) -> anyhow::Result<SlotStatus> {
    ySS::try_from(slot_update.status)
        .map(|v| match v {
            ySS::SlotFirstShredReceived => SlotStatus::FirstShredReceived,
            ySS::SlotProcessed => SlotStatus::Processed,
            ySS::SlotConfirmed => SlotStatus::Confirmed,
            ySS::SlotFinalized => SlotStatus::Rooted,
            ySS::SlotCompleted =>  SlotStatus::Completed,
            ySS::SlotCreatedBank => SlotStatus::CreatedBank,
            ySS::SlotDead => SlotStatus::Dead("n/a".to_string()),
        })
        .map_err(|_| anyhow!("invalid commitment level"))
}
