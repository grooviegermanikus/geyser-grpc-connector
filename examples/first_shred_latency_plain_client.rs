/// logs first shred of slot seen from gRPC - used to compare with validator block production (from logs)

use anyhow::anyhow;
use clap::Parser;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{
    map_commitment_level, GrpcConnectionTimeouts, GrpcSourceConfig, Message,
};
use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::env;
use std::time::Duration;
use futures::{SinkExt, StreamExt};
use solana_sdk::clock::Slot;
use tokio::sync::broadcast;
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{CommitmentLevel as yCL, SlotStatus, SubscribeUpdateSlot};
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterSlots};


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {}

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let _args = Args::parse();

    let grpc_addr = env::var("GRPC_ADDR").expect("need grpc url");
    let grpc_x_token = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source {} ({})",
        grpc_addr,
        grpc_x_token.is_some()
    );

    let mut client = GeyserGrpcClient::build_from_shared(grpc_addr)?
        .x_token(grpc_x_token)?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;
    let (mut subscribe_tx, mut stream) = client.subscribe().await?;

    subscribe_tx
        .send(build_slot_subscription())
        .await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Slot(update_msg)) => {


                        if update_msg.status == SlotStatus::SlotFirstShredReceived as i32 {
                            info!("FIRST_SHRED:{}", update_msg.slot)
                        }


                    }
                    _ => {}
                }
            }
            Err(error) => {
                error!("stream error: {error:?}");
                break;
            }
        }
    }

    Ok(())
}

fn build_slot_subscription() -> SubscribeRequest {
    let slots_sub = HashMap::from([(
        "geyser_tracker_slots_all_levels".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: Some(true),
        },
    )]);

    SubscribeRequest {
        slots: slots_sub,
        ..Default::default()
    }
}

