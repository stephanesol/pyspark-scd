from pyspark.sql.types import *

table_profiles = {
'emp_snapshots' : {
                    'input_path': '../data/input/*.csv',
                    'output_path': '../data/input/processed',
                    'schema' :
                            StructType([
                                        StructField('snapshot_date', DateType(), True), 
                                        StructField('employee_number', IntegerType(), True),
                                        StructField('status', StringType(), True),
                                        StructField('first_name', StringType(), True),
                                        StructField('last_name', StringType(), True),
                                        StructField('gender', StringType(), True),
                                        StructField('email', StringType(), True),
                                        StructField('phone_number', StringType(), True),
                                        StructField('salary', IntegerType(), True),
                                        StructField('termination_date', DateType(), True)
                                    ]),
                    'keys' : ["snapshot_date","employee_number"]
                    },
'employee_all' : {
                    'input_path': '../data/output/employee_all/*.csv',
                    'output_path': '../data/output/employee_all',
                    'schema' :
                            StructType([
                                    StructField('snapshot_date', DateType(), True), 
                                    StructField('employee_number', IntegerType(), True),
                                    StructField('status', StringType(), True),
                                    StructField('first_name', StringType(), True),
                                    StructField('last_name', StringType(), True),
                                    StructField('gender', StringType(), True),
                                    StructField('email', StringType(), True),
                                    StructField('phone_number', StringType(), True),
                                    StructField('salary', IntegerType(), True),
                                    StructField('termination_date', DateType(), True),
                                    StructField('change_status', StringType(), False),
                                    StructField('changed_status_date', DateType(), True)
                                ]),
                    'keys' : ["snapshot_date","employee_number"]
                    },
'employee_current' : {
                    'input_path': '../data/output/employee_current/*.csv',
                    'output_path': '../data/output/employee_current',
                    'schema' :
                            StructType([
                                    StructField('snapshot_date', DateType(), True), 
                                    StructField('employee_number', IntegerType(), True),
                                    StructField('status', StringType(), True),
                                    StructField('first_name', StringType(), True),
                                    StructField('last_name', StringType(), True),
                                    StructField('gender', StringType(), True),
                                    StructField('email', StringType(), True),
                                    StructField('phone_number', StringType(), True),
                                    StructField('salary', IntegerType(), True),
                                    StructField('termination_date', DateType(), True),
                                    StructField('change_status', StringType(), False),
                                    StructField('changed_status_date', DateType(), True)
                                ]),
                    'keys' : ["snapshot_date","employee_number"]
    }
}