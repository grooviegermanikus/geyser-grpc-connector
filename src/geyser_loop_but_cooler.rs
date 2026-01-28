use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::str::FromStr;
use yellowstone_grpc_proto::geyser::{SubscribeUpdateSlot, SlotStatus as ySS, SubscribeUpdate, SlotStatus, CommitmentLevel};
use anyhow::{anyhow, Context};
use log::trace;
use solana_clock::Slot;
use solana_signature::Signature;
use tracing::field::debug;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::prelude::{SubscribeUpdatePing, SubscribeUpdateTransactionStatus};

pub struct MessagesBuffer {
    grpc_updates: Vec<Box<SubscribeUpdate>>,
}

// note: there is still an ordering problem where data might arrive after slot confirmed message was seen
// - only support confirmed level
pub struct GeyserLoopButCooler {

    buffer: BTreeMap<Slot, MessagesBuffer>,

    // maintain a safety window where we emit single messages
    confirmed_slots: BTreeSet<Slot>,

}

pub enum Effect {
    EmitConfirmedMessages { confirmed_slot: Slot, grpc_updates: Vec<Box<SubscribeUpdate>> },
    // some messages might arrive after confirmed slot was seen
    EmitLateConfirmedMessage { confirmed_slot: Slot, grpc_updates: Box<SubscribeUpdate> },
    Noop,
}

// number of slots to emit late messages
const LATE_MESSAGES_SAFETY_WINDOW: u64 = 64;

impl GeyserLoopButCooler {

    pub fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
            confirmed_slots: BTreeSet::new(),
        }
    }

    pub fn consume_move(&mut self, update: SubscribeUpdate) -> anyhow::Result<Effect> {
        self.consume(Box::new(update))
    }

    pub fn consume(&mut self, update: Box<SubscribeUpdate>) -> anyhow::Result<Effect> {

        match update.update_oneof.as_ref() {
            Some(UpdateOneof::Slot(msg)) => {
                let commitment_status = SlotStatus::try_from(msg.status).context("unknown status")?;

                if commitment_status != SlotStatus::SlotConfirmed {
                    return Ok(Effect::Noop);
                }
                let confirmed_slot = msg.slot;
                // lazily fall back to empty list
                let mut messages = self.buffer.remove(&confirmed_slot).unwrap_or( MessagesBuffer{ grpc_updates: Vec::new() });

                messages.grpc_updates.push(update);

                // clean all older slots; could clean up more aggressively but this is safe+good enough
                self.buffer.retain(|&slot, _| slot > confirmed_slot);
                // we don't expect wide span of process slots around in the future
                debug_assert!(self.buffer.len() < 10, "buffer should be small");

                let was_new = self.confirmed_slots.insert(confirmed_slot);
                debug_assert!(was_new, "only one confirmed slot message expected");

                // clean up the window of confirmed_slots
                self.confirmed_slots.retain(|s| s + LATE_MESSAGES_SAFETY_WINDOW >= confirmed_slot);


                trace!("Emit {} messages for slot {}", messages.grpc_updates.len(), confirmed_slot);

                return Ok(Effect::EmitConfirmedMessages { confirmed_slot, grpc_updates: messages.grpc_updates });

            }
            Some(UpdateOneof::Ping(_) | UpdateOneof::Pong(_)) => {
                return Ok(Effect::Noop);
            }
            // all messages except slot (+ping pong)
            Some(msg) => {
                let slot = get_slot(&msg);

                if self.confirmed_slots.contains(&slot) {
                    return Ok(Effect::EmitLateConfirmedMessage { confirmed_slot: slot, grpc_updates: update });
                }

                self.buffer.entry(slot)
                    .or_insert_with(|| MessagesBuffer { grpc_updates: Vec::with_capacity(64) })
                    .grpc_updates.push(update);
            }
            None => {
                // not really expected
            }
        }

        Ok(Effect::Noop)
    }


}

fn get_slot(update: &UpdateOneof) -> Slot {
    match update {
        UpdateOneof::Account(msg) => {
            msg.slot
        }
        UpdateOneof::TransactionStatus(msg) => {
            msg.slot
        }
        UpdateOneof::Transaction(msg) => {
            msg.slot
        }
        UpdateOneof::Block(msg) => {
            msg.slot
        }
        UpdateOneof::BlockMeta(msg) => {
            msg.slot
        }
        UpdateOneof::Entry(msg) => {
            msg.slot
        }
        _ => {
            panic!("unsupported update type for get_slot: {:?}", update);
        }
    }
}

#[test]
pub fn test_simple() {

    let mut cool = GeyserLoopButCooler::new();

    let effect = cool.consume_move(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 41_999_000,
            parent: None,
            status: ySS::SlotConfirmed as i32,
            dead_error: None,
        })),
    });
    let Ok(Effect::EmitConfirmedMessages{confirmed_slot, grpc_updates: grpc_update }) = effect else {
        panic!()
    };
    assert_eq!(confirmed_slot, 41_999_000);
    assert_eq!(grpc_update.len(), 1); // slot message


    let sig1 = Signature::from_str("2h6iPLYZEEt8RMY3gGFUqd4Jktrg2fYTCMffifRoQDJWPqLvZ1gRKqpq4e5s8kWrVigkyDXV6xEiw54zuChYBdyB").unwrap();
    let sig2 = Signature::from_str("5QE2kQUiMpv51seq4ShtoaAzdkMT7fzeQ5TvqTPFgNkcahtHSnZudindggjTUXt8uqZGifbWUAmUubdWLhFHz719").unwrap();
    let sig3 = Signature::from_str("KQzbyZMUq6ujZL6qxDW2EMNUugvzcpFJSdzTnmhsV8rYgqkwL9rc3uXg1FpGPNKaSJQLmKXTfezJoVdBLEhVa8F").unwrap();

    for sig in [sig1, sig2, sig3] {
        let effect = cool.consume_move(SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                slot: 42_000_000,
                signature: sig.as_ref().to_vec(),
                is_vote: false,
                index: 0,
                err: None,
            })),
        });
        assert!(matches!(effect, Ok(Effect::Noop)));
    }

    let effect = cool.consume_move(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
    });
    assert!(matches!(effect, Ok(Effect::Noop)));

    let effect = cool.consume_move(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 42_000_000,
            parent: None,
            status: ySS::SlotProcessed as i32,
            dead_error: None,
        })),
    });
    assert!(matches!(effect, Ok(Effect::Noop)));

    let effect = cool.consume_move(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 42_000_000,
            parent: None,
            status: ySS::SlotConfirmed as i32,
            dead_error: None,
        })),
    });
    assert!(matches!(effect, Ok(Effect::EmitConfirmedMessages{..})));

}


#[test]
pub fn test_emit_late_message() {
    let mut cool = GeyserLoopButCooler::new();

    let sig1 = Signature::from_str("2h6iPLYZEEt8RMY3gGFUqd4Jktrg2fYTCMffifRoQDJWPqLvZ1gRKqpq4e5s8kWrVigkyDXV6xEiw54zuChYBdyB").unwrap();

    let effect = cool.consume_move(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
            slot: 43_000_000,
            signature: sig1.as_ref().to_vec(),
            is_vote: false,
            index: 0,
            err: None,
        })),
    });
    assert!(matches!(effect, Ok(Effect::Noop)));




    let effect = cool.consume_move(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 43_000_000,
            parent: None,
            status: ySS::SlotConfirmed as i32,
            dead_error: None,
        })),
    });
    let Ok(Effect::EmitConfirmedMessages { confirmed_slot, grpc_updates: grpc_update }) = effect else {
        panic!()
    };
    assert_eq!(confirmed_slot, 43_000_000);
    assert_eq!(grpc_update.len(), 2); // tx+slot message

    let sig3 = Signature::from_str("KQzbyZMUq6ujZL6qxDW2EMNUugvzcpFJSdzTnmhsV8rYgqkwL9rc3uXg1FpGPNKaSJQLmKXTfezJoVdBLEhVa8F").unwrap();

    // insert into next slot
    let effect = cool.consume_move(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
            slot: 43_000_001,
            signature: sig3.as_ref().to_vec(),
            is_vote: false,
            index: 0,
            err: None,
        })),
    });
    assert!(matches!(effect, Ok(Effect::Noop)));


    let sig_late = Signature::from_str("5QE2kQUiMpv51seq4ShtoaAzdkMT7fzeQ5TvqTPFgNkcahtHSnZudindggjTUXt8uqZGifbWUAmUubdWLhFHz719").unwrap();
    let effect = cool.consume_move(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
            slot: 43_000_000,
            signature: sig_late.as_ref().to_vec(),
            is_vote: false,
            index: 99,
            err: None,
        })),
    });

    let Ok(Effect::EmitLateConfirmedMessage {confirmed_slot, grpc_updates: grpc_message }) = effect else {
        panic!()
    };
    assert_eq!(confirmed_slot, 43_000_000);
    match grpc_message.update_oneof.unwrap() {
        UpdateOneof::TransactionStatus(tx_status) => {
            assert_eq!(tx_status.index, 99);
        }
        _ => {}
    }

    let sig_verylate = Signature::from_str("5QE2kQUiMpv51seq4ShtoaAzdkMT7fzeQ5TvqTPFgNkcahtHSnZudindggjTUXt8uqZGifbWUAmUubdWLhFHz719").unwrap();
    let effect = cool.consume_move(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
            slot: 42_999_000,
            signature: sig_verylate.as_ref().to_vec(),
            is_vote: false,
            index: 77,
            err: None,
        })),
    });
    assert!(matches!(effect, Ok(Effect::Noop)));

}