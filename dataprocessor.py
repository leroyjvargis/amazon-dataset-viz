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
    for idx, f in enumerate(files):
        filename = directory + f
        category = f.split(".")[0]
        fiftyKdata = pd.DataFrame()
        df = pd.read_json(filename, lines=True, convert_dates=['unixReviewTime'], chunksize=size) #, names=names)
        for each in df:
            df_data = each
            if len(fiftyKdata) == 0:
                fiftyKdata = df_data
            else:
        #         fiftyKdata.append(df_data)
                fiftyKdata = pd.concat([fiftyKdata, df_data], ignore_index=True, names=names)

                fiftyKdata = fiftyKdata.sort_values(by="unixReviewTime", ascending=False)
                fiftyKdata = fiftyKdata.head(size)
            fiftyKdata = add_missing_cols(fiftyKdata)
            
        fiftyKdata['category'] = category
        fiftyKdata = fiftyKdata[names + ['category']]
        fiftyKdata.to_csv('combined_data.csv', mode='a', index=False, header=idx == 0)
        print ("Parsed", category)

def filterByAsin():
    combined_data = pd.read_csv("combined_data.csv") 
    cat_df = combined_data.groupby("category")

    for i in range(len(files)):
        category = files[i].split(".")[0]

        unique_asins = cat_df.get_group(category)["asin"].unique()
        unique_asins_list = list(unique_asins)

        filename_meta = meta_d + meta_files[i]
        df_meta = pd.read_json(filename_meta, lines=True, convert_dates=['unixReviewTime'], chunksize=20000)
        all_df = pd.DataFrame()

        starttime = datetime.now()
        for curr_df in df_meta: 
            filtered_df = curr_df[curr_df['asin'].isin(unique_asins_list)]
            all_df = pd.concat([all_df, filtered_df])
            if all_df['asin'].count() >=len(unique_asins_list):
                break
            
            print ("elapsed:", str(int((datetime.now() - starttime).total_seconds() // 60)), "minutes", str(int((datetime.now() - starttime).total_seconds() %60)), "seconds", "found:", all_df['asin'].count(), end='\r' )
        save_file_name = category.split('_')[0].lower() + '_meta_filtered.csv'
        all_df.to_csv(save_file_name, index=False, header=True)
        


def addProductName():
    combined_data = pd.read_csv("combined_data.csv")
    sports_meta_filtered = pd.read_csv("sports_meta_filtered.csv")
    home_meta_filtered = pd.read_csv("home_meta_filtered.csv")
    electronics_meta_filtered = pd.read_csv("electronics_meta_filtered.csv")
    clothing_meta_filtered = pd.read_csv("clothing_meta_filtered.csv")
    books_meta_filtered = pd.read_csv("books_meta_filtered.csv")

    names = list(books_meta_filtered.columns)
    all_meta = pd.DataFrame()
    all_meta = pd.concat([all_meta, sports_meta_filtered], ignore_index=True, names=names)
    all_meta = pd.concat([all_meta, home_meta_filtered], ignore_index=True, names=names)
    all_meta = pd.concat([all_meta, electronics_meta_filtered], ignore_index=True, names=names)
    all_meta = pd.concat([all_meta, clothing_meta_filtered], ignore_index=True, names=names)
    all_meta = pd.concat([all_meta, books_meta_filtered], ignore_index=True, names=names)

    combined_data_with_title = pd.merge(combined_data,all_meta[['asin','title', 'price']],on='asin', how='left')
    combined_data_with_title.to_csv("combined_data_with_title.csv", index=False)