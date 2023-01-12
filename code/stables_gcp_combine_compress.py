### combine raw daily files into monthly files and compress ###

import os
import glob
import pandas as pd

years = ["2017", "2018", "2019", "2020", "2021", "2022"] 
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

for year in years: 
    for month in months:
        
        batch = "stablecoins_raw_" + year + "-" + month + "-*.csv"

        files = os.path.join("/Users/brendan/dev/onchain_data_blog/raw_data", batch) 
        all_files = glob.glob(files)

        li = []
        
        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0)
            li.append(df)
            
        if len(li) > 0:
    
            full_frame_batch = pd.concat(li, axis=0, ignore_index=True)

            output = month + year + ".csv.gz"
            os.chdir('/Users/brendan/dev/onchain_data_blog/temp_data')
            full_frame_batch.to_csv(output, index=False, compression="gzip")
