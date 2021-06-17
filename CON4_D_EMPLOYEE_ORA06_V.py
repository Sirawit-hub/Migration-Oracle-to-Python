import cx_Oracle
import pandas as pd
from pandas.core.frame import DataFrame
import numpy as np
import datetime

#set format dataframe / เรียงตารางให้ตรง ตาม row และ column
#pd.set_option('display.max_columns', None) #โชว์ columns ทั้งหมด
pd.set_option('display.max_rows', None) #โชว์ rows ทั้งหมด

dsn_tns = cx_Oracle.makedsn("147.50.43.90", 1531, service_name="VIS") # if needed, place an 'r' before any parameter in order to address special characters such as ''.
con = cx_Oracle.connect(user='apps', password='apps', dsn=dsn_tns,encoding="UTF-8") # if needed, place an 'r' before any parameter in order to address special characters such as ''. For example, if your user name contains '', you'll need to place 'r' before the user name: user=r'User Name'
cursor = con.cursor()

#insert data to dataframe
query_t4_1 = "select * from employees " 
df_employees = pd.read_sql(query_t4_1,con)

query_t4_2 = "select * from jobs"
df_jobs = pd.read_sql(query_t4_2,con)

query_t4_3 = "select * from job_history"
df_job_history = pd.read_sql(query_t4_3,con)

query_t4_4 = """select e.EMPLOYEE_ID,m.EMPLOYEE_ID memployee_id,e.MANAGER_ID,e.first_name,m.first_name mfirstname,
m.last_name mlastname from employees e join employees m on e.manager_id = m.employee_id"""
df_manager = pd.read_sql(query_t4_4,con)


#add MANAGER_NAME ด้วยการเชื่อม MFIRSTNAME กับ MLASTNAME
df_manager["MANAGER_NAME"] = (df_manager["MFIRSTNAME"] +" "+ df_manager["MLASTNAME"]).str.strip()
df_manager = df_manager.sort_values(["EMPLOYEE_ID"])
#print(df_manager)

#ดึง EMPLOYEE_ID และ  MANAGER_NAME เก็บไว้ใน df_manager_name
df_manager_name=df_manager[["EMPLOYEE_ID","MANAGER_NAME"]]
#print(df_manager_name)

#--------------------------------join--------------------------------------

#drop empty rows in "MANAGER_ID" column / ตัดค่า null ใน ตาราง
df_employees.dropna(subset = ["MANAGER_ID"], inplace=True)

#sort by column / เรียงคอลัมน์ด้วย "EMPLOYEE_ID"
df_employees = df_employees.sort_values(["EMPLOYEE_ID"])
#print(df_employees)

#left join employees and jobs 109 rows
df_emp_jobs = pd.merge(df_employees, df_jobs, on='JOB_ID', how='left')
#print(df_emp_jobs)

#left join employees, jobs and job_history 112 rows
df_emp_jobs_jh = pd.merge(df_emp_jobs, df_job_history, on='EMPLOYEE_ID', how='left')
#print(df_emp_jobs_jh)

#----------------------------------------------------------------------

#ทำการ join datafram df_emp_jobs_jh กับ df_manager_name เพื่อดึงชื่อ MANAGER_NAME ออกมา
df_emp_jobs_jh = pd.merge(df_emp_jobs_jh,df_manager_name , on='EMPLOYEE_ID',how='left')
#print(df_emp_jobs_jh)


#-----------------------------------------------------------------------------------------------------------
#add EMPLOYEE_NAME to df_emp_jobs_jh -- 21 column
df_emp_jobs_jh["EMPLOYEE_NAME"] = (df_emp_jobs_jh["FIRST_NAME"] +" "+ df_emp_jobs_jh["LAST_NAME"]).str.strip()
#print(df_emp_jobs_jh)

#replace END_DATE with 'NA' และตั้งชื่อคอลัมน์ว่า LAST_DATE
df_emp_jobs_jh["LAST_DATE"] = df_emp_jobs_jh["END_DATE"].replace(np.nan, 'NA')
#print(df_emp_jobs_jh)

#replace COMMISSION_PCT with 0
df_emp_jobs_jh["EMPLOYEE_COMMISSION"] = df_emp_jobs_jh["COMMISSION_PCT"].replace(np.nan,0)
#print(df_emp_jobs_jh)

#concat email "@gmail.com" / ใส่ gamil.com ต่อท้าย EMAIL
df_emp_jobs_jh["EMPLOYEE_EMAIL"] = df_emp_jobs_jh["EMAIL"] +"@gmail.com"
#print(df_emp_jobs_jh)

#replace . -
df_emp_jobs_jh["EMPLOYEE_PHONE_NUMBER"] = df_emp_jobs_jh["PHONE_NUMBER"].str.replace('.','-')
#print(df_emp_jobs_jh)

#---------------------------------------------วันที่------------------------------------------
"""
df_last_date=df_emp_jobs_jh[["EMPLOYEE_ID","EMPLOYEE_NAME","LAST_DATE"]] #,"EMPLOYEE_EMAIL","EMPLOYEE_COMMISSION","MANAGER_NAME","EMPLOYEE_NAME"
df_last_date["LAST_DATE"] = df_last_date["LAST_DATE"].replace("NA", np.nan)
df_last_date.dropna(subset = ["LAST_DATE"], inplace=True)

#ตัด END_DATE  id ที่ ซ้ำออก
df_last_date = df_last_date.sort_values(by="LAST_DATE").drop_duplicates(subset=["EMPLOYEE_ID"],keep="last")
#print(df_last_date)


#df_emp_jobs_jh_test = pd.merge(df_emp_jobs_jh, df_last_date,on='EMPLOYEE_ID', how='left')
#print(df_emp_jobs_jh_test)
"""
#---------------------------------------FULL---------------------------------------------------

#test all dataframe
df_total=df_emp_jobs_jh[["EMPLOYEE_NAME","EMPLOYEE_EMAIL","EMPLOYEE_PHONE_NUMBER","LAST_DATE","JOB_TITLE","EMPLOYEE_COMMISSION","MANAGER_NAME"]]
#df_total = df_total.sort_values(["EMPLOYEE_NAME"])
print(df_total)

#------------------------------------------------------------------------------------------------




"""
#-------------------------โค้ดสำรอง----------------------------------------------------------
df_last_date=df_emp_jobs_jh[["EMPLOYEE_NAME","LAST_DATE"]]

#เปลี่ยนค่า "NA" ให้เป็นค่า NaN
df_last_date["LAST_DATE"] = df_last_date["LAST_DATE"].replace("NA", np.nan)
#ลบ rows ที่มี NaN ทั้งหมด เหลือ 7 rows
df_last_date.dropna(subset = ["LAST_DATE"], inplace=True)
#print(df_last_date)

#จัดแถวตาม LAST_DATE
df_last_date = df_last_date.sort_values(["LAST_DATE"])
#print(df_last_date)


#ลบชื่อ EMPLOYEE_NAME ที่ซ้ำ
#df_last_date.drop_duplicates(subset ="EMPLOYEE_NAME",keep = False, inplace = True)
#print(df_last_date)
"""

#left join total และ 
#df_total = pd.merge(df_total, df_last_date, on='LAST_DATE', how='inner')
#print(df_total)


##------------------------------------------------------------------------------------------------



#select column from dataframe / เลือกคอลัมน์ที่ต้องการบน dataframe
#df_total=df_employees[["EMPLOYEE_ID","EMAIL","PHONE_NUMBER"]]
#print(df_total)


#enddate=df_job_history[["EMPLOYEE_ID","END_DATE"]]
#print(enddate)


#merge column(Full Outer Join) from dataframe /เชื่อม dataframe แบบ Full Outer Join
#df_merge_col = pd.merge(df_employees, enddate, on='EMPLOYEE_ID',how='outer')
#print(df_merge_col)


#join dataframe /เอา dataframe มาชนกันเลย
#df_row = pd.concat([df_employees, df_jobs])
#print(df_row)

#replace . - test / แทนค่า . ด้วย -
#df_phone=df_emp_jobs_jh[["PHONE_NUMBER"]]
#df_phone['PHONE_NUMBER'] = df_phone['PHONE_NUMBER'].str.replace('.','-')
#print(df_phone)

# replace null to 0  and change typt float64 to int64
#df_employees["MANAGER_ID"] = df_employees["MANAGER_ID"].replace(np.nan, 0)
#df_employees["MANAGER_ID"] = df_employees["MANAGER_ID"].apply(np.int64)
#df_employees["MANAGER_ID"] = df_employees["MANAGER_ID"].replace(0,np.nan)


#check type dataframe
#print(df_employees.dtypes)