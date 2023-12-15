use crate::grpc_subscription_autoreconnect::Message;
use crate::grpc_subscription_autoreconnect::Message::GeyserSubscribeUpdate;
use async_stream::stream;
use futures::Stream;
use log::{debug, info, warn};
use merge_streams::MergeStreams;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

pub trait FromYellowstoneMapper {
    // Target is something like ProducedBlock
    type Target;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)>;
}

/// use streams created by ``create_geyser_reconnecting_stream``
/// note: this is agnostic to the type of the stream
pub fn create_multiplex<M>(
    grpc_source_streams: Vec<impl Stream<Item = Message>>,
    commitment_config: CommitmentConfig,
    mapper: M,
) -> impl Stream<Item = M::Target>
where
    M: FromYellowstoneMapper,
{
    assert!(
        commitment_config == CommitmentConfig::confirmed()
            || commitment_config == CommitmentConfig::finalized(),
        "Only CONFIRMED and FINALIZED is supported"
    );
    // note: PROCESSED blocks are not sequential in presense of forks; this will break the logic

    if grpc_source_streams.is_empty() {
        panic!("Must have at least one source");
    }

    info!(
        "Starting multiplexer with {} sources",
        grpc_source_streams.len(),
    );

    // use merge
    // let mut futures = futures::stream::SelectAll::new();

    let mut streams = vec![];
    for grpc_source in grpc_source_streams {
        streams.push(Box::pin(grpc_source));
    }

    let merged_streams = streams.merge();

    map_updates(merged_streams, mapper)
}

fn map_updates<S, E>(merged_streams: S, mapper: E) -> impl Stream<Item = E::Target>
where
    S: Stream<Item = Message>,
    E: FromYellowstoneMapper,
{
    let mut tip: Slot = 0;
    stream! {
        for await update in merged_streams {
            match update {
                GeyserSubscribeUpdate(update) => {
                    // take only the update messages we want
                    if let Some((proposed_slot, block)) = mapper.map_yellowstone_update(update) {
                        if proposed_slot > tip {
                            tip = proposed_slot;
                            yield block;
                        }
                    }
                }
                Message::Reconnecting => {
                    warn!("Stream performs reconnect"); // TODO waht does that mean?
                }
            }
        }
    }
}
