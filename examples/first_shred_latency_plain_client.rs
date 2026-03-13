use clap::Parser;
use futures::{SinkExt, StreamExt};
use log::{error, info};
use std::collections::HashMap;
use std::env;
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SlotStatus;
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

    subscribe_tx.send(build_slot_subscription()).await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => match msg.update_oneof {
                Some(UpdateOneof::Slot(update_msg)) => {
                    if update_msg.status == SlotStatus::SlotFirstShredReceived as i32 {
                        info!("FIRST_SHRED:{}", update_msg.slot)
                    }
                }
                _ => {}
            },
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
