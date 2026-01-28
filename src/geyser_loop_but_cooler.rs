use std::collections::HashMap;
use std::str::FromStr;
use yellowstone_grpc_proto::geyser::{SubscribeUpdateSlot, SlotStatus as ySS, SubscribeUpdate, SlotStatus, CommitmentLevel};
use anyhow::anyhow;
use solana_clock::Slot;
use solana_signature::Signature;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionStatus;

pub struct MessagesBuffer {
    // TODO consolidate naming: grpc messages vs updates
    grpc_messages: Vec<SubscribeUpdate>,
}

pub struct GeyserLoopButCooler {

    skip_til_this_slot: Option<Slot>,
    buffer: HashMap<Slot, MessagesBuffer>,


}

pub enum Effect {
    // confimred slot + messages
    EmitConfirmedMessages { confirmed_slot: Slot, grpc_messages: Vec<SubscribeUpdate> },
    Noop,
}

impl GeyserLoopButCooler {

    pub fn new() -> Self {
        Self {
            skip_til_this_slot: None,
            buffer: HashMap::new(),
        }
    }

    pub fn foobar(&mut self) {
    }

    pub fn consume(&mut self, update: SubscribeUpdate) -> Effect {

        match update.update_oneof.as_ref() {
            // this is important
            Some(UpdateOneof::Slot(msg)) => {
                let commitment_status = SlotStatus::try_from(msg.status).expect("status");

                if self.skip_til_this_slot == None && commitment_status == SlotStatus::SlotProcessed {
                    // special case for proper startup
                    self.skip_til_this_slot = Some(msg.slot);
                    return Effect::Noop;
                }
                if commitment_status == SlotStatus::SlotProcessed {
                    // make sure the messages is there even if it's empty
                    self.buffer.entry(msg.slot)
                        .or_insert_with(|| MessagesBuffer { grpc_messages: Vec::with_capacity(64) });
                    return Effect::Noop;
                }
                if commitment_status != SlotStatus::SlotConfirmed {
                    return Effect::Noop;
                }
                let confirmed_slot = msg.slot;
                if let Some(skip_slot) = self.skip_til_this_slot {
                    if msg.slot <= skip_slot {
                        println!("hold back until slot > {}", skip_slot);
                        return Effect::Noop;
                    }
                } else {
                    println!("hold back until skip_slot is set");
                    return Effect::Noop;
                }

                let messages = self.buffer.remove(&confirmed_slot).expect("must be there");

                println!("Need to flush messages for slot {} {}", confirmed_slot, messages.grpc_messages.len());

                return Effect::EmitConfirmedMessages { confirmed_slot, grpc_messages: messages.grpc_messages  };

            }
            // all messages except slot (+ping pong)
            Some(msg) => {

                match msg {
                    UpdateOneof::Ping(_) => {
                        return Effect::Noop;
                    }
                    UpdateOneof::Pong(_) => {
                        return Effect::Noop;
                    }
                    _ => {}
                }

                let slot = get_slot(&msg);
                self.buffer.entry(slot)
                    .or_insert_with(|| MessagesBuffer { grpc_messages: Vec::with_capacity(64) })
                    .grpc_messages.push(update);
            }
            None => {}
        }

        Effect::Noop
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
pub fn test_gesyer_loop_but_cooler() {

    let mut cool = GeyserLoopButCooler::new();

    let sig1 = Signature::from_str("2h6iPLYZEEt8RMY3gGFUqd4Jktrg2fYTCMffifRoQDJWPqLvZ1gRKqpq4e5s8kWrVigkyDXV6xEiw54zuChYBdyB").unwrap();
    let sig2 = Signature::from_str("5QE2kQUiMpv51seq4ShtoaAzdkMT7fzeQ5TvqTPFgNkcahtHSnZudindggjTUXt8uqZGifbWUAmUubdWLhFHz719").unwrap();
    let sig3 = Signature::from_str("KQzbyZMUq6ujZL6qxDW2EMNUugvzcpFJSdzTnmhsV8rYgqkwL9rc3uXg1FpGPNKaSJQLmKXTfezJoVdBLEhVa8F").unwrap();

    for sig in [sig1, sig2, sig3] {
        let effect = cool.consume(SubscribeUpdate {
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
        assert!(matches!(effect, Effect::Noop));
    }

    let effect = cool.consume(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 41_999_999,
            parent: None,
            status: ySS::SlotProcessed as i32,
            dead_error: None,
        })),
    });
    assert!(matches!(effect, Effect::Noop));

    let effect = cool.consume(SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 42_000_000,
            parent: None,
            status: ySS::SlotConfirmed as i32,
            dead_error: None,
        })),
    });
    assert!(matches!(effect, Effect::EmitConfirmedMessages{..}));

}
