#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::str::FromStr;
use yellowstone_grpc_proto::geyser::{SubscribeUpdateSlot, SlotStatus as ySS, SubscribeUpdate, SlotStatus, CommitmentLevel, SubscribeRequestFilterSlots};
use anyhow::{anyhow, bail, Context};
use log::trace;
use solana_clock::Slot;
use solana_commitment_config::CommitmentConfig;
use solana_signature::Signature;
use tracing::field::debug;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeUpdatePing, SubscribeUpdateTransactionStatus};
use crate::map_commitment_level;

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
    EmitLateConfirmedMessage { confirmed_slot: Slot, grpc_update: Box<SubscribeUpdate> },
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
                if update.filters != vec!["_magic_confirmed_slots".to_string()] {
                    bail!("unexpected slot message with filters: {:?}", update.filters);
                }
                let commitment_status = SlotStatus::try_from(msg.status).context("unknown status")?;

                if commitment_status != SlotStatus::SlotConfirmed {
                    // we do not pass through slot processed+finalized
                    return Ok(Effect::Noop);
                }
                let confirmed_slot = msg.slot;
                // lazily fall back to empty list
                let mut messages = self.buffer.remove(&confirmed_slot).unwrap_or( MessagesBuffer{ grpc_updates: Vec::new() });

                // append the slot message
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
            // all messages except slot (+ping pong)
            Some(msg) => {
                if matches!(msg, UpdateOneof::Ping(_) | UpdateOneof::Pong(_)) {
                    return Ok(Effect::Noop);
                }

                let slot = get_slot(&msg)?;

                if self.confirmed_slots.contains(&slot) {
                    trace!("Emit late message for recently confirmed slot {}", slot);
                    return Ok(Effect::EmitLateConfirmedMessage { confirmed_slot: slot, grpc_update: update });
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

fn get_slot(update: &UpdateOneof) -> anyhow::Result<Slot> {
    Ok(match update {
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
            bail!("unsupported update type for get_slot: {:?}", update);
        }
    })
}


pub fn build_subscription_request(subscription: SubscribeRequest) -> SubscribeRequest {

    // as slots with filter_by_commitment = false

    // TODO don't allow slot
    // TODO add our slot subscription
    // TODO commitment levev

    assert!(subscription.slots.is_empty(), "will implicitly send confirmed slots");

    let magic_slots_subscription = SubscribeRequestFilterSlots { filter_by_commitment: None, interslot_updates: None };

    SubscribeRequest {
        slots: HashMap::from([
            ("_magic_confirmed_slots".to_string(), magic_slots_subscription),
        ]),
        commitment: Some(map_commitment_level(CommitmentConfig::processed()) as i32),
        ..subscription
    }

}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use std::path::Path;
    use yellowstone_grpc_proto::geyser::{SubscribeUpdateSlot, SlotStatus as ySS, SubscribeUpdate, SlotStatus, CommitmentLevel, SubscribeUpdateTransactionStatus, SubscribeUpdatePing, SubscribeUpdateTransactionInfo, SubscribeUpdateTransaction, SubscribeUpdateAccount};

    use std::str::FromStr;
    use anyhow::anyhow;
    use itertools::Itertools;
    use log::trace;
    use solana_clock::Slot;
    use solana_pubkey::Pubkey;
    use solana_signature::Signature;
    use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
    use yellowstone_grpc_proto::prelude::{Message, SubscribeUpdateAccountInfo, TransactionStatusMeta};
    use crate::geyser_loop_but_cooler::{Effect, GeyserLoopButCooler};

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

        let Ok(Effect::EmitLateConfirmedMessage {confirmed_slot, grpc_update: grpc_message }) = effect else {
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

    #[test]
    #[ignore] // requires fixture file
    pub fn test_massive_replay() {
        let mut cool = GeyserLoopButCooler::new();

        let lines = read_lines_from_file("../fixtures/geyser_loop/geyser-trace-100k.csv").unwrap();
        let iter = read_messages_from_csv_with_line_no(lines, Some(1)).unwrap().into_iter()
            .map(|(update, ..)| update).collect_vec();

        let count_non_slot_per_slot: HashMap<Slot, u64> = HashMap::from([(1212, 12)]);


        for update in iter {
            let subscribe_update = SubscribeUpdate {
                filters: vec![],
                created_at: None,
                update_oneof: Some(update),
            };
            let result = cool.consume_move(subscribe_update).unwrap();

            match result {
                Effect::EmitConfirmedMessages { confirmed_slot, grpc_updates } => {
                    println!("from slot {} got {} messages", confirmed_slot, grpc_updates.len());
                }
                Effect::EmitLateConfirmedMessage { confirmed_slot, grpc_update } => {
                    println!("from slot {} got late message", confirmed_slot);
                }
                Effect::Noop => {}
            }

        }

    }



    pub fn read_messages_from_csv(
        source: impl Iterator<Item = String>,
        filter_source_idx: Option<u32>,
    ) -> anyhow::Result<Vec<(UpdateOneof, SourceIdx)>> {
        read_messages_from_csv_with_line_no(source, filter_source_idx).map(|vec| {
            vec.into_iter()
                .map(|(msg, source_idx, _line_no)| (msg, source_idx))
                .collect_vec()
        })
    }


    pub fn read_messages_from_csv_with_line_no(
        source: impl Iterator<Item = String>,
        filter_source_idx: Option<u32>,
    ) -> anyhow::Result<Vec<(UpdateOneof, SourceIdx, usize)>> {
        let mut result_vec = Vec::new();

        let regex_dumpslot = regex::Regex::new(r"DUMPSLOT (\d+),(\d+),(\d+),(\w+),(\d+)").unwrap();
        let regex_dumpaccount = regex::Regex::new(r"DUMPACCOUNT (\d+),(\d+),(\w+),(\d+)").unwrap();
        let regex_dumptx = regex::Regex::new(r"DUMPTX (\d+),(\d+),(\w+),(\d+)").unwrap(); // removed last field which was index

        let random_pubkey_owner = Pubkey::new_unique().as_ref().to_vec();

        for (line_no, line) in source.enumerate() {
            if line.starts_with("DUMPSLOT") {
                let (_full_match, [source_idx, slot, parent_slot, short_status, epoch_ms]) =
                    regex_dumpslot.captures(&line).unwrap().extract();
                trace!(
                    "captures {},{},{},{},{}",
                    source_idx,
                    slot,
                    parent_slot,
                    short_status,
                    epoch_ms
                );

                let source_idx = source_idx.parse::<u32>().unwrap();
                if let Some(filter_source_idx) = filter_source_idx {
                    if source_idx != filter_source_idx {
                        continue;
                    }
                }

                let slot = slot.parse::<u64>().unwrap();
                let parent_slot = parent_slot.parse::<u64>().unwrap();
                let parent = if parent_slot == 0 {
                    None
                } else {
                    Some(parent_slot)
                };
                let short_status = short_status.to_string();

                let status = match short_status.as_str() {
                    "P" => SlotStatus::SlotProcessed,
                    "C" => SlotStatus::SlotConfirmed,
                    "F" => SlotStatus::SlotFinalized,
                    _ => panic!("unknown short_status: {}", short_status),
                } as i32;

                let msg = UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot,
                    parent,
                    status,
                    dead_error: None,
                });
                result_vec.push((msg, SourceIdx::new(source_idx), line_no));
            }
            if line.starts_with("DUMPACCOUNT") {
                let (_full_match, [source_idx, slot, account_pk, epoch_ms]) =
                    regex_dumpaccount.captures(&line).unwrap().extract();

                let source_idx = source_idx.parse::<u32>().unwrap();
                if let Some(filter_source_idx) = filter_source_idx {
                    if source_idx != filter_source_idx {
                        continue;
                    }
                }

                let slot = slot.parse::<u64>().unwrap();
                let pubkey = Pubkey::from_str(account_pk).unwrap();

                trace!(
                    "captures {},{},{},{}",
                    source_idx,
                    slot,
                    account_pk,
                    epoch_ms
                );

                let update = SubscribeUpdateAccountInfo {
                    pubkey: pubkey.as_ref().to_vec(),
                    lamports: 420000,
                    owner: random_pubkey_owner.clone(),
                    executable: false,
                    rent_epoch: 0,
                    data: vec![],
                    write_version: (33_000_000 + line_no) as u64,
                    txn_signature: None,
                };
                result_vec.push((
                    UpdateOneof::Account(
                        // SubscribeUpdateAccount::from_update_oneof(update, timestamp_now()).unwrap(),
                        SubscribeUpdateAccount {
                            account: Some(update),
                            slot,
                            is_startup: false,
                        }
                    ),
                    SourceIdx::new(source_idx),
                    line_no,
                ));
            }
            if line.starts_with("DUMPTX") {
                let (_full_match, [source_idx, slot, sig, epoch_ms]) =
                    regex_dumptx.captures(&line).expect(&line).extract();

                let source_idx = source_idx.parse::<u32>().unwrap();
                if let Some(filter_source_idx) = filter_source_idx {
                    if source_idx != filter_source_idx {
                        continue;
                    }
                }

                let slot = slot.parse::<u64>().unwrap();
                // let index = index.parse::<u64>().unwrap();
                let index = 4242;
                let signature = Signature::from_str(sig).unwrap();

                // trace!(
                //     "captures {},{},{},{},{}",
                //     source_idx,
                //     slot,
                //     sig,
                //     epoch_ms,
                //     index
                // );

                let message = yellowstone_grpc_proto::prelude::Message {
                    header: None,
                    account_keys: vec![],
                    recent_blockhash: vec![],
                    instructions: vec![],
                    versioned: false,
                    address_table_lookups: vec![],
                };

                let tx = yellowstone_grpc_proto::prelude::Transaction {
                    signatures: vec![],
                    message: Some(message),
                };

                let meta = TransactionStatusMeta {
                    err: None,
                    fee: 0,
                    pre_balances: vec![],
                    post_balances: vec![],
                    inner_instructions: vec![],
                    inner_instructions_none: false,
                    log_messages: vec![],
                    log_messages_none: false,
                    pre_token_balances: vec![],
                    post_token_balances: vec![],
                    rewards: vec![],
                    loaded_writable_addresses: vec![],
                    loaded_readonly_addresses: vec![],
                    return_data: None,
                    return_data_none: false,
                    compute_units_consumed: None,
                    cost_units: None,
                };

                let tx_info = SubscribeUpdateTransactionInfo {
                    signature: signature.as_ref().to_vec(),
                    is_vote: false,
                    transaction: Some(tx),
                    meta: Some(meta),
                    index,
                };

                let update = SubscribeUpdateTransaction {
                    transaction: Some(tx_info),
                    slot,
                };
                result_vec.push((
                    UpdateOneof::Transaction(update),
                    SourceIdx::new(source_idx),
                    line_no,
                ));
            }
        }

        return Ok(result_vec);
    }

    pub fn read_lines_from_file<P>(filename: P) -> anyhow::Result<impl Iterator<Item = String>>
    where
        P: AsRef<Path>,
    {
        let file = File::open(filename).map_err(|err| anyhow!("error reading file: {}", err))?;
        Ok(BufReader::new(file).lines().map_while(Result::ok))
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct SourceIdx(u32);

    impl SourceIdx {
        /// 1-based index
        #[inline]
        pub const fn new(idx: u32) -> Self {
            assert!(idx > 0, "source index is 1-based");
            assert!(idx < 5 as u32, "source index too big");
            SourceIdx(idx)
        }

        // turn 1-based source_idx into array index
        pub fn idx0(&self) -> usize {
            (self.0 - 1) as usize
        }
    }

}
