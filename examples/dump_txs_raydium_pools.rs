use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use bincode::deserialize;
use itertools::Itertools;
use log::info;
use solana_account_decoder::parse_address_lookup_table::UiLookupTable;
use solana_account_decoder::UiAccountEncoding;
use solana_rpc_client::rpc_client::RpcClientConfig;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use solana_rpc_client_api::response::Response;
use solana_sdk::account::Account;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::address_lookup_table::state::AddressLookupTable;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::example_mocks::solana_rpc_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use tokio::sync::broadcast;
use tokio::time::Duration;
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeUpdateTransactionInfo};

use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{
    map_commitment_level, GrpcConnectionTimeouts, GrpcSourceConfig, Message,
};

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let grpc_addr_green = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token_green = env::var("GRPC_X_TOKEN").ok();

    let (_foo, exit_notify) = broadcast::channel(1);

    info!(
        "Using gRPC source {} ({})",
        grpc_addr_green,
        grpc_x_token_green.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(15),
        request_timeout: Duration::from_secs(15),
        subscribe_timeout: Duration::from_secs(15),
        receive_timeout: Duration::from_secs(15),
    };

    let tls_config = ClientTlsConfig::new().with_native_roots();
    let green_config =
        GrpcSourceConfig::new(grpc_addr_green, grpc_x_token_green, Some(tls_config), timeouts.clone());

    let (autoconnect_tx, mut transactions_rx) = tokio::sync::mpsc::channel(10);
    let _tx_source_ah = create_geyser_autoconnection_task_with_mpsc(
        green_config.clone(),
        raydium_initialize_pools(),
        autoconnect_tx.clone(),
        exit_notify,
    );

    loop {
        let message = transactions_rx.recv().await;
        if let Some(Message::GeyserSubscribeUpdate(update)) = message {
            match update.update_oneof {
                Some(UpdateOneof::Transaction(update)) => {
                    let tx: SubscribeUpdateTransactionInfo = update.transaction.unwrap();

                    parse_tx(tx);

                }
                _ => {},
            }
        }
    }
}

const RAYDIUM_CPMM_PUBKEY: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
fn raydium_initialize_pools() -> SubscribeRequest {
    let mut transaction_subs = HashMap::new();
    transaction_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![
                // "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string(),
                // "AutobNFLMzX1rFCDgwWpwr3ztG5c1oDbSrGq7Jj2LgE".to_string(),
                // "DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH".to_string(),
                RAYDIUM_CPMM_PUBKEY.to_string(),
            ],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeRequest {
        transactions: transaction_subs,
        ping: None,
        commitment: Some(map_commitment_level(CommitmentConfig {
            commitment: CommitmentLevel::Confirmed,
        }) as i32),
        ..Default::default()
    }
}

fn alt_resolve(alt_pubkey: Pubkey) -> Vec<Pubkey> {
    let rpc_client = solana_rpc_client::rpc_client::RpcClient::new("https://api.mainnet-beta.solana.com".to_string());
    // let rpc_client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());

    // UiLookupTable
    let commitment_config = CommitmentConfig::confirmed();
    let config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::JsonParsed),
        commitment: Some(commitment_config),
        .. RpcAccountInfoConfig::default()
    };

    // println!("lookup alt: {}", alt_pubkey);
    let alt_account = rpc_client.get_account(&alt_pubkey).unwrap();
    let lookup_table = solana_sdk::address_lookup_table::state::AddressLookupTable::deserialize(&alt_account.data).unwrap();

    // println!("lookup alt account.len: {}", alt_account.data.len());


    lookup_table.addresses.to_vec()

}

fn parse_tx(tx: SubscribeUpdateTransactionInfo) {
    let sig = Signature::try_from(tx.signature.as_slice()).unwrap();

    let message = tx.transaction.unwrap().message.unwrap();

    let mut account_keys = Vec::new();
    for key in message.account_keys {

        let bytes: [u8; 32] =
            key.try_into().unwrap();

        account_keys.push(
        Pubkey::new_from_array(bytes)
        );

    }

    // TODO optimize - do after chekc


    let cpmm_pubkey = Pubkey::from_str_const(RAYDIUM_CPMM_PUBKEY);
    let alt_from_tx  = &message.address_table_lookups;

    // taken from raydium-cp-swap instruction::Initialize::DISCRIMINATOR
    let discriminator: [u8; 8] = [175, 175, 109, 31, 13, 152, 155, 237];
    // taken from raydium-cp-swap  PoolState::DISCRIMINATOR
    // let discriminator: [u8; 8] = [247, 237, 227, 245, 215, 195, 222, 70];
    for ins in message.instructions {
        if let Some(disc) = ins.data.get(..8) {
            if account_keys[ins.program_id_index as usize] == cpmm_pubkey && disc == discriminator {
                println!("MATCH by discriminator for tx {}", sig);
                assert_eq!(ins.accounts.len(), 20);

                for alt in alt_from_tx {
                    let alt_pubkey = Pubkey::new_from_array(alt.account_key.as_slice().try_into().unwrap());

                    let alt_addresses = alt_resolve(alt_pubkey);

                    for alt in &alt_addresses {
                        // println!("- alt address: {}", alt);

                    }


                    for wi in &alt.writable_indexes {

                        account_keys.push(
                            alt_addresses[*wi as usize]
                        );
                    }
                    for ri in &alt.readonly_indexes {
                        account_keys.push(
                            alt_addresses[*ri as usize]
                        );
                    }
                }

                for (i, acc_key) in account_keys.iter().enumerate() {
                    println!("Account #{}: {}", i + 1, acc_key);
                }

                println!("ins account: {:?}", ins.accounts);

                // 5. Token 0 Mint
                let token0_mint = account_keys[ins.accounts[4] as usize];
                println!("Token0 Mint: {}", token0_mint);
                // 6. Token 1 Mint
                let token1_mint = account_keys[ins.accounts[5] as usize];
                println!("Token1 Mint: {}", token1_mint);



                // let token_0_mint = Pubkey::new_from_array(ins.data[(5*32)..(5*32+32)].try_into().unwrap());
                // let token_1_mint = Pubkey::new_from_array(ins.data[(6*32)..(6*32+32)].try_into().unwrap());
                // println!("Token0 Mint: {}", token_0_mint);
                // println!("Token1 Mint: {}", token_1_mint);

                // Token0 Mint
                // let token0_mint = account_keys[4];
                // Token1 Mint
                // let token1_mint = account_keys[5];

                // println!("Token0 Mint: {}", token0_mint);
                // println!("Token1 Mint: {}", token1_mint);
                //



            }
        }
        // ins.
    }

    // info!("tx {} with {} accounts", sig, account_keys.len());


}