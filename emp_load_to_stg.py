import sys
sys.path.append('/Users/pradeep/Downloads/Udemy_course_videos/course_2_assignments/Snowpark_pipeline/')
from generic_code import code_library
from schema import source_schema
from snowflake.snowpark.context import get_active_session


connection_parameters = {"account":"kp41433.ap-southeast-1", \
"user":"pavan", \
"password": "Abc123123", \
"role":"ACCOUNTADMIN", \
"warehouse":"COMPUTE_WH", \
"database":"DEMO_DB", \
"schema":"PUBLIC" \
}

config_file = {"Database_name":"DEMO_DB",\
"Schema_name":"PUBLIC",
"Target_table":"EMPLOYEE",
"Reject_table":"EMPLOYEE_REJECTS",
"target_columns":["FIRST_NAME","LAST_NAME","EMAIL","ADDRESS","CITY","DOJ"],
"on_error":"CONTINUE",
"Source_location":"@my_s3_stage/employee/",
"Source_file_type":"csv"
}
    
# Declare schema for csv file and read data
schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
StructField("DOJ",DateType())])

session = code_library.snowconnection(connection_parameters)
copied_into_result, qid = code_library.copy_to_table(session,config_file,schema)



print(copied_into_result)
print(qid)

copied_into_result_df = session.create_dataframe(copied_into_result)
copied_into_result_df.show()







f = open('/Users/pradeep/Downloads/Udemy_course_videos/course_2_assignments/Snowpark_etl/config/copy_to_snowflake.json', "r")
config = json.loads(f.read())





rejects.count()

get_active_session()

print(copied_into_result)

print(qid)

import pandas as pd
from snowflake.connector.options import installed_pandas, pandas

from snowflake.connector.options import installed_pandas

from snowflake.connector.options import installed_pandas

df = session.create_dataframe(pd.DataFrame([(1, 2, 3, 4)], columns=["a", "b", "c", "d"])).collect()

print(df)

df.show()

df = session.table("DEMO_DB.PUBLIC.SNOWPARK_TEMP_TABLE_GL8Z56B6A4")
#pip install "snowflake-connector-python[pandas]"

if  installed_pandas:
    print("hi")