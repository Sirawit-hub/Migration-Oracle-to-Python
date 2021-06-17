import cx_Oracle
import pandas as pd
import mysql.connector
 

dsn_tns = cx_Oracle.makedsn("147.50.43.90", 1531, service_name="VIS") # if needed, place an 'r' before any parameter in order to address special characters such as ''.
con = cx_Oracle.connect(user='apps', password='apps', dsn=dsn_tns,encoding="UTF-8") # if needed, place an 'r' before any parameter in order to address special characters such as ''. For example, if your user name contains '', you'll need to place 'r' before the user name: user=r'User Name'

cursor = con.cursor()

cursor.execute("CREATE TABLE  D_DEPARTMENT_ORA06 (DEPARTMENT_CD	 		NUMBER(4),\
	DEPARTMENT_NAME	 		NVARCHAR2(100),\
	STREET_ADDRESS	 		NVARCHAR2(30), \
	POSTAL_CODE	 		    NVARCHAR2(12),\
	CITY				    NVARCHAR2(30),\
	STATE_PROVINCE	 		NVARCHAR2(25),\
	COUNTRY_NAME			NVARCHAR2(40),\
	REGION_NAME_EN			NVARCHAR2(25),\
	REGION_NAME_TH			NVARCHAR2(25),\
	ETL_DATE			    TIMESTAMP(6),\
	ETL_LAST_UPDATE			TIMESTAMP(6))")

print("create table D_DEPARTMENT_ORA06 success")