use log::info;
use solana_clock::Slot;
use solana_transaction::versioned::VersionedTransaction;
use tokio::sync::mpsc;

pub fn main() {}

pub async fn startup(mut rx: mpsc::Receiver<(Slot, VersionedTransaction)>) {
    let mut startup_slot = None::<Slot>;
    let mut requested_slot = None::<Slot>;
    let mut transactions: Vec<VersionedTransaction> = vec![];

    while let Some((slot, tx)) = rx.recv().await {
        let Some(startup_slot) = startup_slot else {
            startup_slot = Some(slot);
            continue;
        };

        if slot == startup_slot {
            continue;
        }

        let requested_slot = *requested_slot.get_or_insert(slot);

        if slot != requested_slot {
            info!("DONE");
            break;
        }

        transactions.push(tx);
    }
}
