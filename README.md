# blockchain-indexer

Indexes all Circles related transactions on the xDai blockchain from block 12529458 (deployment of the Circles Hub Contract) onwards in a postgres database. It provides no api to query the database.

## Quickstart
1) Create a new database with [schema](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/Schema.sql) on a postgres server
2) Pull the docker image: `docker pull ghcr.io/circlesland/blockchain-indexer:[IMAGE_VERSION]`
3) Run with
   ```shell
   docker run --name=blockchain_indexer -d \ 
      -e INDEXER_CONNECTION_STRING='Server=[DATABASE_SERVER];Port=[DATABASE_PORT];Database=[DATABASE_NAME];User ID=[DATABASE_USER];Password=[DATABASE_PASSWORD];Command Timeout=120;' \
      -e INDEXER_RPC_GATEWAY_URL='[RPC_GATEWAY_URL]' \
      -e INDEXER_WEBSOCKET_URL='http://0.0.0.0:8675/' \
      -p"8675:8675" \
      ghcr.io/circlesland/blockchain-indexer:[IMAGE_VERSION]
   ```

## Indexed events
* crc_hub_transfer
* crc_signup
* crc_organisation_signup
* crc_trust
* erc20_transfer
* eth_transfer
* gnosis_safe_eth_transfer

## Websocket server
The indexer creates a websocket server to which any client can connect to receive a stream of the last indexed transaction hashes.  
Example message:  
```json
["0xa457608171336e4dccbef7ef436a2769904a850d03a379c852bbb95f52dae582","0x0104b20fd0ed4655c2427881f1e2bbd2b655ad4669b359837da5b1ef72f58656","0x486fd78a556005073cebd5692e0ebd387e5e6d898e39a7257aceddb6d558f2f0","0xa8979b035c79acc408786856241afbc53c4c2213ad552dd6fc9fa36ec2bad536","0x2de1c915ac4872795050f9085c58451a878603e25ae745f64c130ac6038a7e6f","0xd9cdd53ce5f5e7998bdc0323c7770c346048ab48c8f84dd1e394151979f31a7d","0xac54d87e4965e474fd5996c0fd4729fae121836090e1a46761309a19dc052176","0x8e2bb90ee547902e53392f4433110b57ddcb813af9ca948ef7fc04b114249655","0xbd8a432ca4e40a00953f9f268a921ec87dafd2e7c6da0766855816eb1e66d791","0x375d12f40caaef02ef0db8a24eeed1c54ab88fa2b5a48a26af36abbcb82b917e","0x344ea59f582690b70ce12b74aad9a8842cda467ad62afd5da486f2a2d091eada","0x343d5fb4ddec41319178521822c0eda06a76cdc49d9c0b43427f2c9819db3931","0x2674194ff89daddaed09de96baf7128401efe1c031b43e65148bf3d581725b43","0xfa78330d4de0d742f286d5fcf6f2a26c28322fa6156dc1202034628a877d2d34","0xd721d7a5ec7f5ec83fe0162ade62758d4af26cd50d1467d546d8674ceb82f61f","0xe947a66859a365f391166907746a2635cd7254cc19ce0c3c2b329f8909ed572b","0x41c42d20c170e57245908a01e3828cace349bd4359510ff2711556dd3a868863","0xa5d686ecb48eddcd3cbec8b234109891f03d5df4c2762d90dd812405522f40ad","0x8a967f11472850714cda9f55cb7e4aea5a998fe92fedd791662ed54bb9fc10d8","0x08184f71daf1e240b6f1a7a70e3856cce0c547cd3d3fb80c2d658b553fe56f17","0x9a406be95c1a92989c04eb18da60acf84fbc3a2d8019292e66eeba2e52b65c46","0x023465a298509ec27c2e20a7afaf38ecbd5b0131d12f33156ce00ba71f82d30c"]
```
The websocket server will only yield events when the IntervalSource is active.

## How it works
### BlockSource
There are two different types of sources: One for bulk imports and another polling-source which periodically asks the rpc gateway for new blocks.
The first one is used to update a large block backlog. The second one is used to keep the database up to date.
* [BulkSource](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/Sources/BulkSource.cs)
* [IntervalSource](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/Sources/IntervalSource.cs)
* [LiveSource (incomplete)](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/Sources/LiveSource.cs)

### Extractors
The block sources download complete blocks. This way all transactions of a downloaded block could be processed in one go. 
However since there seems to be no standard rpc-method to download all receipts for a block, these are downloaded one by one for each transaction.  

When a block is completely downloaded it is passed downstream to the following extractors which will create 0..n TransactionDetailModels for it:
* [CrcHubTransferDetailExtractor](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/DetailExtractors/CrcHubTransferDetailExtractor.cs)
* [CrcSignupDetailExtractor](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/DetailExtractors/CrcOrganisationSignupDetailExtractor.cs)
* [CrcOrganisationSignupDetailExtractor](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/DetailExtractors/CrcOrganisationSignupDetailExtractor.cs)
* [CrcTrustDetailExtractor](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/DetailExtractors/CrcTrustDetailExtractor.cs)
* [Erc20TransferDetailExtractor](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/DetailExtractors/Erc20TransferDetailExtractor.cs)
* [EthTransferDetailExtractor](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/DetailExtractors/EthTransferDetailExtractor.cs)
* [GnosisSafeEthTransferDetailExtractor](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/DetailExtractors/GnosisSafeEthTransferDetailExtractor.cs)

### Staging tables
The transactions and the extracted details are then written to dedicated staging tables. Its o.k. for staging tables to have duplicate entries. The data in these
tables is kept until there is a confirmed row in the corresponding indexed database table (see "import_from_staging"). The schema is identical to the final indexed tables but without indexes.  

All (bulk)insert statements can be found in the [StagingTables.cs](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/Persistence/StagingTables.cs) file.

### Import from staging
The actual import from the staging tables into the indexed main-schema is done by the [import_from_staging()](https://github.com/circlesland/blockchain-indexer/blob/0aba70b57e5702292b684a1603258bdf0fd64747/CirclesLand.BlockchainIndexer/Schema.sql#L956) stored procedure.  
When the bulk-source is active this procedure is only called from time to time. When the InvervalSource is active it is called after every new block.

## Availability, Reliability, Consistency and Health checks
**Availability, Reliability**  
It is possible to run multiple instances of the blockchain-indexer simultaneously (e.g. with different rpc-gateways). If both instances are healthy they will both write the same data to the staging tables. However only one instance can call [import_from_staging()](https://github.com/circlesland/blockchain-indexer/blob/0aba70b57e5702292b684a1603258bdf0fd64747/CirclesLand.BlockchainIndexer/Schema.sql#L956) at the same time. This is achieved by using a [Serializable](https://www.postgresql.org/docs/9.5/transaction-iso.html#XACT-SERIALIZABLE) database transaction. If two processes call the procedure at the same time then one of them will fail and in consequence restart its processing loop.

If a process expieriences an error it logs it and then restart it's [processing loop](https://github.com/circlesland/blockchain-indexer/blob/14a368a2eb03b8aad2f94f7196951fc27eab4172/CirclesLand.BlockchainIndexer/Indexer..cs#L50). A increasing dynamic back-off time [is applied](https://github.com/circlesland/blockchain-indexer/blob/14a368a2eb03b8aad2f94f7196951fc27eab4172/CirclesLand.BlockchainIndexer/Indexer..cs#L60) after each error. The max waiting time is limited to [2 minutes](https://github.com/circlesland/blockchain-indexer/blob/14a368a2eb03b8aad2f94f7196951fc27eab4172/CirclesLand.BlockchainIndexer/Settings.cs#L13). 

Depending on the mode (bulk or live) the import_from_staging()-procedure is called with different timeouts:  
* CatchUp: 120 sec.
* all others: 10 sec.

**Consistency**  
The [import_from_staging()](https://github.com/circlesland/blockchain-indexer/blob/0aba70b57e5702292b684a1603258bdf0fd64747/CirclesLand.BlockchainIndexer/Schema.sql#L956) procedure works in three steps and two stages:
1) Mark rows in staging tables:
1.1) All rows that form a complete block (number of distinct transactions equals the block's total_transaction_count).
1.2) All rows that already exist in the indexed tables.
2) Import all marked rows (only in subsequent calls)
3) Delete all rows from the staging table that are marked with "already existing"  
Everything is matched by it's hash but the contents are not validated.

**Health checks**   
There is no built in mechanism for health checks but it should be easy to listen to the transaction hashes and define a timeout and alert after N-seconds without new transactions.

## Known issues
* Initially puts heavy load on the rpc-gateway because it downloads all blocks with 24 parallel connections and receipts with 96 parallel connections (should be replaced with direct ingest from a geth/netermind/etc. db)
* Not configurable yet. Settings are baked into [Settings.cs](https://github.com/circlesland/blockchain-indexer/blob/main/CirclesLand.BlockchainIndexer/Settings.cs) and the software needs to be recompiled 
* Doesn't validate blocks
* Uses a lot of threadpool threads and waits for some of them somewhere. This can cause thread pool starvation during the bulk import. 4 cores are adviced during this phase.
