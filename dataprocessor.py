import pandas as pd
from datetime import datetime 

directory = '/media/leroy/OS/School/_CSE 578/Project/'
files = ['Sports_and_Outdoors_5.json', 'Home_and_Kitchen_5.json', 'Electronics_5.json', 'Clothing_Shoes_and_Jewelry_5.json', 'Books_5.json']
size = 50000

meta_d = '/media/leroy/OS/School/_CSE 578/Project/meta/'
meta_files = ['meta_Sports_and_Outdoors.json', 'meta_Home_and_Kitchen.json', 'meta_Electronics.json', 'meta_Clothing_Shoes_and_Jewelry.json', 'meta_Books.json']

names=["overall", "verified", "reviewTime", "reviewerID", "asin", "reviewerName", "reviewText", "summary", "unixReviewTime", "style", "vote", "image"]
def add_missing_cols(df):
    for each in names:
        if each not in df.columns:
            df[each] = None
    return df

def get50kdata():
    ## get top 50K (based on review date) from each category
    ## doing it this way since data is too large to hold at once in memory
    for idx, f in enumerate(files):
        filename = directory + f
        category = f.split(".")[0]
        fiftyKdata = pd.DataFrame()

        # read data in chunks, to get a serialized object
        df = pd.read_json(filename, lines=True, convert_dates=['unixReviewTime'], chunksize=size) #, names=names)
        
        # iterate over each chunk of 50k data each
        for each in df:
            df_data = each
            if len(fiftyKdata) == 0:
                fiftyKdata = df_data
            else:
                fiftyKdata = pd.concat([fiftyKdata, df_data], ignore_index=True, names=names)

                # keep only top 50k from this iteration
                fiftyKdata = fiftyKdata.sort_values(by="unixReviewTime", ascending=False)
                fiftyKdata = fiftyKdata.head(size)
            fiftyKdata = add_missing_cols(fiftyKdata)
            
        fiftyKdata['category'] = category
        fiftyKdata = fiftyKdata[names + ['category']]

        # append to file. 
        fiftyKdata.to_csv('combined_data.csv', mode='a', index=False, header=idx == 0)
        print ("Parsed", category)

def get100kdata():
    ## get 100K from each category
    ## filter data to be only from 2017

    directory = '/media/leroy/OS/School/_CSE 578/Project/'
    files = ['Sports_and_Outdoors_5.json', 'Home_and_Kitchen_5.json', 'Electronics_5.json', 'Clothing_Shoes_and_Jewelry_5.json', 'Books_5.json']
    size = 50000
    start_date = "2017-1-1"
    end_date = "2017-12-31"

    starttime = datetime.now()
    for idx, f in enumerate(files):
        filename = directory + f
        category = f.split(".")[0]
        fiftyKdata = pd.DataFrame()
        df = pd.read_json(filename, lines=True, convert_dates=['unixReviewTime'], chunksize=size) #, names=names)
        for each in df:
            # filter to include only those from 2017
            after_start_date = each["unixReviewTime"] >= start_date
            before_end_date = each["unixReviewTime"] <= end_date
            between_two_dates = after_start_date & before_end_date
            df_data = each.loc[between_two_dates]
            
            if len(fiftyKdata) == 0:
                fiftyKdata = df_data
            else:
                fiftyKdata = pd.concat([fiftyKdata, df_data], ignore_index=True, names=names)
                
                # keep only 100K, sample randomly so data is not skewed
                if fiftyKdata['asin'].count() > 100000:
                    fiftyKdata = fiftyKdata.sample(100000)
            fiftyKdata = add_missing_cols(fiftyKdata)
            print ("elapsed:", str(int((datetime.now() - starttime).total_seconds() // 60)), "minutes", str(int((datetime.now() - starttime).total_seconds() %60)), "seconds", "found:", fiftyKdata['asin'].count(), end='\r' )
            
        fiftyKdata['category'] = category
        fiftyKdata = fiftyKdata[names + ['category']]
        savefilename = "filtered_data/" + category.split('_')[0] +'_filtered_data.csv'
        fiftyKdata.to_csv(savefilename, mode='a', index=False, header=True)
        print ("Finished parsing:", category)
        
        


def filterByAsin():
    ## filter meta_data files to include only those products who appear in combined_data
    ## doing it this way cos data is too large to hold in-memory, and since products repeat. 

    # read the previously generated combined data file
    combined_data = pd.read_csv("combined_data.csv") 
    cat_df = combined_data.groupby("category")

    # iterate over each category separately, since meta file is separate
    for i in range(len(files)):
        category = files[i].split(".")[0]

        # get unique asins of products current category
        unique_asins = cat_df.get_group(category)["asin"].unique()
        unique_asins_list = list(unique_asins)

        # read corresponding meta data file in chunks
        filename_meta = meta_d + meta_files[i]
        df_meta = pd.read_json(filename_meta, lines=True, convert_dates=['unixReviewTime'], chunksize=20000)
        all_df = pd.DataFrame()

        starttime = datetime.now()
        for curr_df in df_meta: 
            # filter current chunk by asins in current category
            filtered_df = curr_df[curr_df['asin'].isin(unique_asins_list)]
            all_df = pd.concat([all_df, filtered_df])
            if all_df['asin'].count() >=len(unique_asins_list):
                break
            
            print ("elapsed:", str(int((datetime.now() - starttime).total_seconds() // 60)), "minutes", str(int((datetime.now() - starttime).total_seconds() %60)), "seconds", "found:", all_df['asin'].count(), end='\r' )
        
        # save to separate file for each category
        save_file_name = category.split('_')[0].lower() + '_meta_filtered.csv'
        all_df.to_csv(save_file_name, index=False, header=True)
        


def addProductName():
    ## to add product name and category, using previously generated combined data and previously got unique asins meta data

    # read each file
    combined_data = pd.read_csv("combined_data.csv")
    sports_meta_filtered = pd.read_csv("sports_meta_filtered.csv")
    home_meta_filtered = pd.read_csv("home_meta_filtered.csv")
    electronics_meta_filtered = pd.read_csv("electronics_meta_filtered.csv")
    clothing_meta_filtered = pd.read_csv("clothing_meta_filtered.csv")
    books_meta_filtered = pd.read_csv("books_meta_filtered.csv")

    # add contents of each file to single df
    names = list(books_meta_filtered.columns)
    all_meta = pd.DataFrame()
    all_meta = pd.concat([all_meta, sports_meta_filtered], ignore_index=True, names=names)
    all_meta = pd.concat([all_meta, home_meta_filtered], ignore_index=True, names=names)
    all_meta = pd.concat([all_meta, electronics_meta_filtered], ignore_index=True, names=names)
    all_meta = pd.concat([all_meta, clothing_meta_filtered], ignore_index=True, names=names)
    all_meta = pd.concat([all_meta, books_meta_filtered], ignore_index=True, names=names)
    all_meta.drop_duplicates(subset=['asin'], inplace=True)
    all_meta.reset_index(inplace=True, drop=True)

    # keep only these columns from main data
    filtered_columns = ['overall', 'reviewTime', 'reviewerID', 'asin', 'reviewerName', 'reviewText', 'summary', 'unixReviewTime', 'category']

    # add product name and price to combined_data df
    # combined_data_with_title = pd.merge(combined_data,all_meta[['asin','title', 'price']],on='asin', how='left')
    # combined_data_with_title.to_csv("combined_data_with_title.csv", index=False)

    # add product name, price and brand to main_data
    combined_filtered_data_with_metadata = pd.merge(combined_data[filtered_columns] ,all_meta[['asin','title', 'price', 'brand']],on='asin', how='left')
    combined_filtered_data_with_metadata.to_csv('filtered_data/combined_filtered_with_metadata.csv', index=False, header=True)