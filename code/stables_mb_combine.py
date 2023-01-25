### combine clean daily files into one file ###

import os
import glob
import pandas as pd
        
files = os.path.join("/Users/brendan/dev/onchain_data_blog/temp_data", "stablecoins_*") 
all_files = glob.glob(files)

li = []
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)
            
    full_frame_batch = pd.concat(li, axis=0, ignore_index=True)

    os.chdir('/Users/brendan/dev/onchain_data_blog/clean_data')
    full_frame_batch.to_csv("stables_mb_clean.csv", index=False)
