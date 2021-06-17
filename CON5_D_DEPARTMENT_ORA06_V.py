import cx_Oracle
import pandas as pd
from pandas.core.frame import DataFrame
import numpy as np

#set format dataframe / เรียงตารางให้ตรง ตาม row และ column
#pd.set_option('display.max_columns', None) #โชว์ columns ทั้งหมด
pd.set_option('display.max_rows', None) #โชว์ rows ทั้งหมด

dsn_tns = cx_Oracle.makedsn("147.50.43.90", 1531, service_name="VIS") # if needed, place an 'r' before any parameter in order to address special characters such as ''.
con = cx_Oracle.connect(user='apps', password='apps', dsn=dsn_tns,encoding="UTF-8") # if needed, place an 'r' before any parameter in order to address special characters such as ''. For example, if your user name contains '', you'll need to place 'r' before the user name: user=r'User Name'
cursor = con.cursor()


#insert data to dataframe
query_t5_1 = "select * from departments" 
df_departments_t5_1 = pd.read_sql(query_t5_1,con)

query_t5_2 = "select * from locations" 
df_locations_t5_2 = pd.read_sql(query_t5_2,con)

query_t5_3 = "select * from countries" 
df_countries_t5_3 = pd.read_sql(query_t5_3,con)

query_t5_4 = "select * from regions" 
df_regions_t5_4 = pd.read_sql(query_t5_4,con)

#print(df_departments_t5_1)
#print(df_locations_t5_2)
#print(df_countries_t5_3)
#print(df_regions_t5_4)


df_dep_loc = pd.merge(df_departments_t5_1, df_locations_t5_2, left_on='LOCATION_ID', right_on='LOCATION_ID', how='inner')
#print(df_dep_loc)

df_loc_con = pd.merge(df_dep_loc, df_countries_t5_3, on='COUNTRY_ID', how='left')
#print(df_loc_con)

df_con_reg = pd.merge(df_loc_con, df_regions_t5_4, on='REGION_ID', how='left')



#เปลี่ยนชื่อคอลัมน์
df_con_reg.rename(columns = {"REGION_NAME": "REGION_NAME_EN"}, inplace=True)


#ตัดตัวอักศรให้ไม่เลย 30 ตัวอักศร
for row in df_con_reg:
   df_con_reg["STREET_ADDRESS"] =  df_con_reg["STREET_ADDRESS"].str.slice(0,30)
   


#ตั้งเงื่อนไงเพื่อ add ชื่อทวีปภาษาไทย ลงใน REGION_NAME_TH
df_con_reg.loc[ df_con_reg.REGION_ID == 1, 'REGION_NAME_TH' ] = 'ยุโรป'
df_con_reg.loc[ df_con_reg.REGION_ID == 2, 'REGION_NAME_TH' ] = 'อเมริกา'
df_con_reg.loc[ df_con_reg.REGION_ID == 3, 'REGION_NAME_TH' ] = 'เอเชีย'
df_con_reg.loc[ df_con_reg.REGION_ID == 4, 'REGION_NAME_TH' ] = 'ตะวันออกกลาง และ แอฟริกา'



#ถ้าเป็นค่า NULL (NaN) ให้ใส่คำว่า "NA"
df_con_reg["REGION_NAME_EN"] = df_con_reg["REGION_NAME_EN"].replace(np.nan, "NA")
df_con_reg["REGION_NAME_TH"] = df_con_reg["REGION_NAME_TH"].replace(np.nan, "NA")


#เก็บค่าที่ต้องการใน df_total_05
df_total_05=df_con_reg[["STREET_ADDRESS","REGION_NAME_EN","REGION_NAME_TH"]]
print(df_total_05)

