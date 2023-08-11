import time, sqlite3
import pandas as pd
import psutil


def print_ram_usage():
    RAM = psutil.virtual_memory()
    print(f"Total RAM: {RAM.total / (1024 ** 3):.2f} GB")
    print(f"Available RAM: {RAM.available / (1024 ** 3):.2f} GB")
    print(f"Used RAM: {RAM.used / (1024 ** 3):.2f} GB")
    print(f"Percentage Used: {RAM.percent:.2f}%")


def Drop_Duplicates(data):
    data['null_count']=data.isnull().sum(axis=1)
    data.sort_values(by='null_count', inplace=True)
    data.drop_duplicates(subset=["ts_code", "trade_date"], keep='last',inplace=True)
    data.drop(['null_count'],axis=1,inplace=True)
    return data

def Process_Chunk1(chunksize=5000):  #Change chunksize based on your own RAM
    conn.execute("DROP TABLE IF EXISTS Processed;")
    def each_chunk(chunk):
        Drop_Duplicates(chunk).to_sql('Processed', conn, if_exists='append', index=False)
    list(map(each_chunk, pd.read_sql_query("SELECT * FROM Trading", conn, chunksize=chunksize)))



def Process_Chunk2(chunksize=5000):  #Change chunksize based on your own RAM
    conn.execute("DROP TABLE IF EXISTS Processed;")
    def each_chunk(chunk):
        Drop_Duplicates(chunk).to_sql('Processed', conn, if_exists='append', index=False)
    for chunk in pd.read_sql_query("SELECT * FROM Trading", conn, chunksize=chunksize):
        yield  each_chunk(chunk)

for _ in Process_Chunk2(): pass

#Database Operation ONE:
conn = sqlite3.connect("/home/ec2-user/Trading0712.sqlite")

start_time = time.time()
Process_Chunk1()
print("【Execution time for map():】\n", time.time() - start_time)

start_time = time.time()
print("【Execution time for yield()】:\n", time.time() - start_time)

conn.commit()
conn.close()




def Time_range():
    print("Time range of the data:", data['trade_date'].min(), "to", data['trade_date'].max())

def Data_missing_rate():
    pd.set_option("display.max_columns", 500)
    print("missing_rate_all_years:\n",(data.isnull().sum(axis=0) / data.shape[0]))
    null_count = data.isnull().groupby(data.ann_date.str[:4]).sum()
    total_rows = data.groupby(data.ann_date.str[:4]).size()
    missing_rate_per_year = null_count.divide(total_rows, axis=0)
    print("Null rate per column for each year:\n",missing_rate_per_year)

def Duplicates_CHECK():
    print("Yes, there are duplicate rows" if data.duplicated().any() else "No, there are no duplicate rows")

#Database Operation TWO:
conn = sqlite3.connect("/home/ec2-user/Financial0809.sqlite")
cursor = conn.cursor()
cursor.execute("SELECT * FROM [Financial in loop]")
data = pd.DataFrame(data=cursor.fetchall(), columns=[col[0] for col in cursor.description])  # Convert to DataFrame

Time_range(data)
Data_missing_rate(data)
Duplicates_CHECK(data)
data.to_sql(name='Financial in loop', con=conn,if_exists="replace",index=False)

conn.commit()
conn.close()
