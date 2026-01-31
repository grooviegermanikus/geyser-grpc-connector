use geyser_grpc_looper::geyser_looper::{Effect, GeyserLooper};
use log::info;
use solana_signature::Signature;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use crate::Message;

// TODO add exit_notify
pub fn start_geyser_loop_adapter(
    mut messages_rx: tokio::sync::mpsc::Receiver<Message>,
    mpsc_downstream_tx: tokio::sync::mpsc::Sender<Message>,
) {


    tokio::spawn(async move {

        let mut cool = GeyserLooper::new();

        '_recv_loop: loop {
            match messages_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(update)) => {

                    let what_to_do = cool.consume(update).unwrap();
                    match what_to_do {
                        Effect::EmitConfirmedMessages { confirmed_slot, grpc_updates: grpc_messages } => {
                            let mut count: usize = 0;
                            for msg in grpc_messages {
                                // TODO handle close
                                let result = mpsc_downstream_tx.send(Message::GeyserSubscribeUpdate(msg)).await;
                                if let Err(_closed) = result {
                                    break '_recv_loop;
                                }
                            }
                            info!("cool: slot {} had {} tx statuses", confirmed_slot, count);
                        }
                        Effect::EmitLateConfirmedMessage { confirmed_slot, grpc_update } => {
                            info!("cool: slot {} had late tx status", confirmed_slot);
                            // TODO handle close
                            let result = mpsc_downstream_tx.send(Message::GeyserSubscribeUpdate(grpc_update)).await;
                            if let Err(_closed) = result {
                                break '_recv_loop;
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

}