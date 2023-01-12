import os
import pandas as pd
import numpy as np



os.chdir('/Users/brendan/dev/onchain_data_blog/temp_data')
df = pd.read_csv("062020.csv.gz", compression="gzip", low_memory=False, index_col=None, header=0)

# parse topics column into separate columns             
df['topics'] = df['topics'].str[1:-1]
df = df.join(df['topics'].str.split(expand=True))
df = df.rename(index=str, columns={0:'topic_0', 1 :'topic_1', 2 :'topic_2'}) 
df.drop(['topics'], axis=1)

# remove quotes from topics 
l = [0,1,2]
for i in l:
    col = 'topic_' + str(i)
    df[col] = df[col].str[1:-1]

# add clean labels for stablecoin names 
df['stablecoin'] = df['address']
df['stablecoin'] = df['stablecoin'].str.replace('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', r'USDC', regex=True)
df['stablecoin'] = df['stablecoin'].str.replace('0xdac17f958d2ee523a2206206994597c13d831ec7', r'USDT', regex=True)
df['stablecoin'] = df['stablecoin'].str.replace('0x4fabb145d64652a948d72533023f6e7a623c7c53', r'Binance USD', regex=True)
df['stablecoin'] = df['stablecoin'].str.replace('0x8e870d67f660d95d5be530380d0ec0bd388289e1', r'Pax Dollar', regex=True)
df['stablecoin'] = df['stablecoin'].str.replace('0x056fd409e1d7a124bd7017459dfea2f387b6d5cd', r'Gemini Dollar', regex=True)

#rename columns 
df = df.rename(columns={'size':'block_size','address':'contract_address'}) 

# non-Gemini transactions
mints = {'0xab8530f87dc9b59234c4623bf917212bb2536d647574c8e7e5da92c2ede0c9f8':'mint', 
         '0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a':'mint',
         '0xf5c174d57843e57fea3c649fdde37f015ef08750759cbee88060390566a98797':'mint'}

burns = {'0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5':'burn',
         '0x702d5967f45f6513a38ffc42d6ba9bf230bd40e8f53b16363c7eb4fd2deb9a44':'burn',
         '0x1b7e18241beced0d7f41fbab1ea8ed468732edbcb74ec4420151654ca71c8a63':'burn'} 

df['action'] = df['topic_0']
df.replace({'action':mints}, inplace=True)
df.replace({'action':burns}, inplace=True)

# Gemini transcations 
df['action'].mask((df['stablecoin'] == 'Gemini Dollar') & (df['action'] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") & (df['topic_1'] == "0x0000000000000000000000000000000000000000000000000000000000000000"), 'mint', inplace = True)
df['action'].mask((df['stablecoin'] == 'Gemini Dollar') & (df['action'] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") & (df['topic_2'] == "0x0000000000000000000000000000000000000000000000000000000000000000"), 'burn', inplace = True)

# drop other obs
df = df[df['action'].isin(['mint','burn'])]

# for non USDP/BUSD, divide by 6 decimals; for Pax Dollar and Binance Dollar divide by 18 decimals because they log raw amounts 
df = df[df.data != "0x"] 
df['data'] = df['data'].apply(int, base=16)

def log_amount_to_USD(contract_address: str, amount: int):
    if contract_address == "0x8e870d67f660d95d5be530380d0ec0bd388289e1":
        return amount / 1e18
    elif contract_address == "0x4fabb145d64652a948d72533023f6e7a623c7c53":
        return amount / 1e18
    else:
        return amount / 1e6

df['amount_usd'] = np.vectorize(log_amount_to_USD, otypes=[float])(df['contract_address'], df['data'])

# redorder columns
df = df[['log_index','transaction_index','transaction_hash','block_number','block_hash','block_timestamp','miner','block_size','gas_used',
        'gas_limit','base_fee_per_gas','contract_address','stablecoin','data','topic_0','topic_1','topic_2']]

