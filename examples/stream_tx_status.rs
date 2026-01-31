use clap::Parser;
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::{create_geyser_autoconnection_task_geyser_loop, create_geyser_autoconnection_task_with_mpsc};
use geyser_grpc_connector::{
    map_commitment_level, GrpcConnectionTimeouts, GrpcSourceConfig, Message,
};
use log::info;
use solana_commitment_config::CommitmentConfig;
use solana_signature::Signature;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use geyser_grpc_looper::geyser_looper::{Effect, GeyserLooper};
use geyser_grpc_looper::LooperSubscribeRequest;
use itertools::Itertools;
use solana_clock::Slot;
use solana_pubkey::Pubkey;
use tokio::join;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdate};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let _args = Args::parse();

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source green on {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    let tls_config = ClientTlsConfig::new().with_native_roots();

    let green_config = GrpcSourceConfig::new(
        grpc_addr_green,
        grpc_x_token_green,
        Some(tls_config.clone()),
        timeouts.clone(),
    );

    let (autoconnect1_tx, mut messages1_rx) = tokio::sync::mpsc::channel(10);
    let (autoconnect2_tx, mut messages2_rx) = tokio::sync::mpsc::channel(10);

    let (_exit, exit_notify) = broadcast::channel(1);

    let my_wallet = Pubkey::from_str("ENysnWXFmvqZoeATS1kRwk9JViiNwJM1fdKgrMpZ5TWV").unwrap();
    info!("Filtering tx status for wallet: {}", my_wallet);

    let _green_stream_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        build_tx_status_subscription(my_wallet),
        autoconnect1_tx.clone(),
        exit_notify.resubscribe(),
    );

    let _blue_stream_ah = create_geyser_autoconnection_task_geyser_loop(
        green_config.clone(),
        build_tx_status_subscription_cool(my_wallet),
        autoconnect2_tx.clone(),
        exit_notify.resubscribe(),
    );

    let task1 = tokio::spawn(async move {

        let mut count_per_slot: BTreeMap<Slot, u32> = BTreeMap::new();

        '_recv_loop: loop {
            match messages1_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => {

                    match update.update_oneof {
                        Some(UpdateOneof::TransactionStatus(msg)) => {
                            let slot = msg.slot;
                            let sig = Signature::try_from(msg.signature.as_slice()).unwrap();

                            // info!("tx status {}: {}", slot, sig);

                            if !count_per_slot.contains_key(&slot) {
                                if let Some(last) = count_per_slot.iter().max() {
                                    // info!("slot {} had {} tx statuses", last.0, last.1);
                                }
                            }
                            count_per_slot.entry(slot).and_modify(|c| *c += 1).or_insert(1);


                        }
                        Some(_) => {}
                        None => {}
                    }},
                None => {
                    log::warn!("multiplexer channel closed - aborting");
                    return;
                }
                Some(Message::Connecting(_)) => {}
            }
        }

    });


    let task2 = tokio::spawn(async move {


        let mut cool = GeyserLooper::new();

        '_recv_loop: loop {
            match messages2_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => {

                    let what_to_do = cool.consume(update).unwrap();
                    match what_to_do {
                        Effect::EmitConfirmedMessages { confirmed_slot, grpc_updates: grpc_messages } => {
                            let mut count: usize = 0;
                            for msg in grpc_messages {

                                match msg.update_oneof {
                                    Some(UpdateOneof::TransactionStatus(msg)) => {
                                        let slot = msg.slot;
                                        let sig = Signature::try_from(msg.signature.as_slice()).unwrap();
                                        info!("cool tx {}: {:?}", slot, sig);
                                        count += 1;
                                    }
                                    _ => {}
                                }
                            }
                            info!("cool: slot {} had {} tx statuses", confirmed_slot, count);
                        }
                        Effect::EmitLateConfirmedMessage { confirmed_slot, grpc_update } => {
                            info!("cool: slot {} had late tx status", confirmed_slot);
                            match grpc_update.update_oneof {
                                Some(UpdateOneof::TransactionStatus(msg)) => {
                                    let slot = msg.slot;
                                    let sig = Signature::try_from(msg.signature.as_slice()).unwrap();
                                    info!("cool tx {}: {:?}", slot, sig);
                                }
                                _ => {}
                            }
                        }
                        Effect::Noop => {}
                    }

                },
                None => {
                    log::warn!("multiplexer channel closed - aborting");
                    return;
                }
                Some(Message::Connecting(_)) => {}
            }
        }

    });

    // tokio::spawn(async move {
    //
    //     loop {
    //
    //         let count_naiv = total_naiv_read.load(std::sync::atomic::Ordering::Relaxed);
    //         let total_cool = total_cool_read.load(std::sync::atomic::Ordering::Relaxed);
    //
    //         println!("Total tx statuses processed: naiv={} cool={}", count_naiv, total_cool);
    //
    //
    //         sleep(Duration::from_secs(3)).await;
    //
    //     }
    //
    // });

    join!(task1, task2);

}

fn build_tx_status_subscription(_wallet: Pubkey) -> SubscribeRequest {
    let mut transactions_status_subs = HashMap::new();
    transactions_status_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            // include failed tx as we shouldn't send them again
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeRequest {
        transactions_status: transactions_status_subs,
        commitment: Some(map_commitment_level(CommitmentConfig::confirmed()) as i32),
        ..Default::default()
    }
}


fn build_tx_status_subscription_cool(_wallet: Pubkey) -> LooperSubscribeRequest {

    let mut important_slots_sub = HashMap::new();

    // important_slots_sub.insert(
    //     "slots".to_string(),
    //     SubscribeRequestFilterSlots {
    //         filter_by_commitment: None,
    //         interslot_updates: None,
    //     },
    // );

    let mut transactions_status_subs = HashMap::new();
    transactions_status_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            // include failed tx as we shouldn't send them again
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeRequest {
        slots: important_slots_sub,
        transactions_status: transactions_status_subs,
        commitment: Some(map_commitment_level(CommitmentConfig::processed()) as i32),
        ..Default::default()
    }.try_into().unwrap()
}
