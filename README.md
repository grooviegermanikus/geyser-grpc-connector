

## Solana Geyser gRPC Multiplexing and Reconnect
This project provides multiplexing of multiple [Yellowstone gRPC](https://github.com/rpcpool/yellowstone-grpc) subscriptions based on _Fastest Wins Strategy_.

* Multiple _Futures_ get **merged** where the first next block that arrives will be emitted.
* No __guarantees__ are made about if the messages are continuous or not.
* __Reconnects__ are handled transparently inside the _Futures_.

Disclaimer: The library is designed with the needs of
[LiteRPC](https://github.com/blockworks-foundation/lite-rpc) in mind
yet might be useful for other projects as well.

The implementation is based on _Rust Futures_.

Please open an issue if you have any questions or suggestions ->  [New Issue](https://github.com/blockworks-foundation/geyser-grpc-connector/issues/new).

## Versions
These are the currently maintained versions of the library: [see Wiki](https://github.com/blockworks-foundation/geyser-grpc-connector/wiki)

## Installation and Usage

```cargo add geyser-grpc-connector ```


An example how to use the library is provided in `stream_blocks_mainnet_stream.rs`.

## Known issues
* Library does not support other data than Blocks/Slots very well.
* Should not be used with commitment level __PROCESSED__ because slot numbers are not monotoic.
* Library needs messages to be in order and provide slot information to work properly.

## Overhead
Using this library vs using the `GeyserGrpcClient` directly has some overhead ```but``` it's negligible:

```
GeyserGrpcClient
2025-09-25T09:50:40.960949Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143450
2025-09-25T09:50:41.265481Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143451
2025-09-25T09:50:41.631390Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143452
2025-09-25T09:50:41.999025Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143453
2025-09-25T09:50:42.381589Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143454
2025-09-25T09:50:42.784193Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143455
2025-09-25T09:50:43.113931Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143456
2025-09-25T09:50:43.483731Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143457
2025-09-25T09:50:43.848205Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143458
2025-09-25T09:50:44.211674Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143459
2025-09-25T09:50:44.670032Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143460

geyser-grpc-connector
2025-09-25T09:50:40.960949Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143450
2025-09-25T09:50:41.265481Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143451
2025-09-25T09:50:41.631390Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143452
2025-09-25T09:50:41.999025Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143453
2025-09-25T09:50:42.381589Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143454
2025-09-25T09:50:42.784193Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143455
2025-09-25T09:50:43.113931Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143456
2025-09-25T09:50:43.483731Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143457
2025-09-25T09:50:43.848205Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143458
2025-09-25T09:50:44.211674Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143459
2025-09-25T09:50:44.670032Z  INFO first_shred_latency_plain_client: FIRST_SHRED:369143460
```

The comparison can be reproduced with `examples/first_shred_latency_plain_client.rs` vs `examples/first_shred_latency.rs`.
