use std::collections::HashMap;
use std::str::FromStr;
use yellowstone_grpc_proto::geyser::{SubscribeUpdateSlot, SlotStatus as ySS, SubscribeUpdate, SlotStatus, CommitmentLevel};
use anyhow::anyhow;
use solana_clock::Slot;
use solana_signature::Signature;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionStatus;

pub struct MessagesBuffer {
    tx_status_messages: Vec<Signature>,
}

pub struct GeyerLoopButCooler {

    skip_til_this_slot: Option<Slot>,
    buffer: HashMap<Slot, MessagesBuffer>,


}

pub enum Effect {
    EmitConfirmedMessages(Vec<Signature>),
    Noop,
}

impl GeyerLoopButCooler {

    pub fn new() -> Self {
        Self {
            skip_til_this_slot: None,
            buffer: HashMap::new(),
        }
    }

    pub fn consume(&mut self, update: &SubscribeUpdate) -> Effect {

        match update.update_oneof.as_ref() {
            // this is important
            Some(UpdateOneof::Slot(msg)) => {
                let commitment_status = CommitmentLevel::try_from(msg.status).unwrap();
                if self.skip_til_this_slot == None && commitment_status == CommitmentLevel::Processed {
                    self.skip_til_this_slot = Some(msg.slot);
                    return Effect::Noop;
                }
                if commitment_status != CommitmentLevel::Confirmed {
                    return Effect::Noop;
                }
                if let Some(skip_slot) = self.skip_til_this_slot {
                    if msg.slot <= skip_slot {
                        println!("hold back until slot > {}", skip_slot);
                        return Effect::Noop;
                    }
                } else {
                    println!("hold back until skip_slot is set");
                    return Effect::Noop;
                }

                let messages = self.buffer.remove(&msg.slot).expect("must be there");

                println!("Need to flush messages for slot {} {}", msg.slot, messages.tx_status_messages.len());

                return Effect::EmitConfirmedMessages(messages.tx_status_messages)

            }
            Some(UpdateOneof::TransactionStatus(msg)) => {
                let slot = msg.slot;
                let sig = Signature::try_from(msg.signature.as_slice()).unwrap();
                println!("Transaction status received for slot {} sig {}", slot, sig);
                self.buffer.entry(slot)
                    .or_insert_with(|| MessagesBuffer { tx_status_messages: Vec::with_capacity(64) })
                    .tx_status_messages.push(sig);
            }
            Some(_) => {}
            None => {}
        }

        Effect::Noop
    }


}



#[test]
pub fn test_gesyer_loop_but_cooler() {

    let mut cool = GeyerLoopButCooler::new();

    let sig1 = Signature::from_str("2h6iPLYZEEt8RMY3gGFUqd4Jktrg2fYTCMffifRoQDJWPqLvZ1gRKqpq4e5s8kWrVigkyDXV6xEiw54zuChYBdyB").unwrap();
    let sig2 = Signature::from_str("5QE2kQUiMpv51seq4ShtoaAzdkMT7fzeQ5TvqTPFgNkcahtHSnZudindggjTUXt8uqZGifbWUAmUubdWLhFHz719").unwrap();
    let sig3 = Signature::from_str("KQzbyZMUq6ujZL6qxDW2EMNUugvzcpFJSdzTnmhsV8rYgqkwL9rc3uXg1FpGPNKaSJQLmKXTfezJoVdBLEhVa8F").unwrap();

    for sig in [sig1, sig2, sig3] {
        let effect = cool.consume(&SubscribeUpdate {
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

    let effect = cool.consume(&SubscribeUpdate {
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

    let effect = cool.consume(&SubscribeUpdate {
        filters: vec![],
        created_at: None,
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 42_000_000,
            parent: None,
            status: ySS::SlotConfirmed as i32,
            dead_error: None,
        })),
    });
    assert!(matches!(effect, Effect::EmitConfirmedMessages(_)));

}
