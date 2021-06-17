import cx_Oracle
import pandas as pd
import mysql.connector
 

dsn_tns = cx_Oracle.makedsn("147.50.43.90", 1531, service_name="VIS") # if needed, place an 'r' before any parameter in order to address special characters such as ''.
con = cx_Oracle.connect(user='apps', password='apps', dsn=dsn_tns,encoding="UTF-8") # if needed, place an 'r' before any parameter in order to address special characters such as ''. For example, if your user name contains '', you'll need to place 'r' before the user name: user=r'User Name'

cursor = con.cursor()

cursor.execute("CREATE TABLE D_EMPLOYEE_ORA06 (employee_cd     NUMBER(6),\
    employee_name          NVARCHAR2(200),\
    employee_email         NVARCHAR2(50),\
    employee_phone_number  NVARCHAR2(200),\
    hire_date              DATE,\
    last_date              NVARCHAR2(50),\
    job_title              NVARCHAR2(35),\
    employee_commission    NUMBER(2, 2),\
    manager_cd             NUMBER(6),\
    manager_name           NVARCHAR2(200),\
    etl_date               TIMESTAMP(6),\
    etl_last_update        TIMESTAMP(6))")

print("create table D_EMPLOYEE_ORA06 success")

