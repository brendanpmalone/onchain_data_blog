import os
from datetime import date
import pandas as pd
from google.cloud import bigquery

### basic query ###
def bigquery(q):
    project_id='hazel-thunder-358822'
    df = pd.io.gbq.read_gbq(q, project_id = project_id, dialect = 'standard')
    return df

### parameterize query by date ###

# specify the start and end dates in window "w"
w = pd.date_range(start='9/29/2022', end='9/30/2022')
  
# loop over days in window "w"
for i in w:
    query_date = i.date() 

    query = f'''SELECT
      eth_logs.*, 
      eth_blocks.number, eth_blocks.miner, eth_blocks.size, eth_blocks.gas_limit, eth_blocks.gas_used, eth_blocks.base_fee_per_gas
    FROM
      `bigquery-public-data.crypto_ethereum.logs` AS eth_logs
    LEFT JOIN
      `bigquery-public-data.crypto_ethereum.blocks` AS eth_blocks
    ON
      eth_logs.block_number = eth_blocks.number
    WHERE(
        eth_logs.address='0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'-- USDC
        OR eth_logs.address ='0xdac17f958d2ee523a2206206994597c13d831ec7' -- USDT
        OR eth_logs.address ='0x4fabb145d64652a948d72533023f6e7a623c7c53' -- Binance USD
        OR eth_logs.address = '0x8e870d67f660d95d5be530380d0ec0bd388289e1' -- Pax Dollar
        OR eth_logs.address = '0x056fd409e1d7a124bd7017459dfea2f387b6d5cd' -- Gemini Dollar
        )
      AND DATE(eth_logs.block_timestamp) = '{query_date}' '''
    
    filename = "stablecoins_raw_" + str(query_date) + ".csv"
    
    print("Starting query for " + str(query_date) + "...") 
    
    os.chdir('/Users/brendan/Data Projects/raw_data')
    df = bigquery(query)
    df.to_csv(filename, index=False)
    
    print("Done with query for " + str(query_date) + "!") 

print("*** DONE WITH BATCH ***")
        
