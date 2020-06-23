import glob
import os
import shutil
import sys
from os import path

sys.path.append("../") # go to parent dir
import configs.config as config

import pyspark
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *

class CustomError(Exception):
    """Allows user to create custom errors. """
    pass

def transform(self, f):
    """Allows piping of transformations. 
    :param f: function to apply to df
    :return df: returns a transformed DF.
    """
    return f(self)

DataFrame.transform = transform

def load_csv_df(tp_dict):
    """Loads CSV to DF from input path specified in table profile dictionary.
    :param tp_dict: A table profile dictionary.
    :return df: 
    :return file_list: the list of files that were used in the import.
    """

    file_list = glob.glob(tp_dict['input_path'])
    
    file_count = len(file_list)
    
    df = spark.read \
        .schema(tp_dict['schema']) \
        .format("csv") \
        .option("mode","FAILFAST") \
        .option("header", "true") \
        .option("nullValue","NULL") \
        .option("dateFormat","yyyy-MM-dd") \
        .load(file_list) 
    
    return df, file_list

def get_latest_snapshot(df):
    """Creates employee_current by selecting the latest snapshot from employee_all. 
    :param df: The employee_all staging df.
    :return df: 
    """

    window = Window.partitionBy(["employee_number"]).orderBy(F.col("snapshot_date").desc())
    constWindow = Window.partitionBy(F.lit(1))
    
    df = df \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col("row_number") == 1 ) \
        .withColumn("snapshot_date",F.max("snapshot_date").over(constWindow)) \
        .drop(F.col("row_number"))
    
    return df

def import_new_files(df):
    """Combines the input files to the base table and removes duplicates.
    :param df:
    :return df: 
    """

    window = Window.partitionBy(["snapshot_date","employee_number"]).orderBy(F.col("snapshot_date").desc())
    
    df = df \
        .select(input_DF.columns) \
        .unionAll(input_DF) \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col("row_number") == 1) \
        .drop(F.col("row_number"))
    
    return df

def remove_columns(df):
    """Removes columns from input data frame. e.g. PII
    :param df:
    :return df: 
    """

    remove_col_list = []
    select_cols_list = [x for x in df.columns if x not in remove_col_list]

    df = df \
        .select(*select_cols_list)
    
    return df

def add_row_hash(df):
    """Adds a row hash to detect record changes.
    :param df:
    :return df: 
    """

    hash_columns = [x for x in df.columns if x not in ["snapshot_date"]]
    df = df \
        .withColumn("row_hash", F.sha2(F.concat_ws("||", *hash_columns), 256))

    return df

def get_change_status(df):
    """Calculates the change status and change date for each record e.g (New, Change, No Change, Deleted)
    :param df:
    :return df: 
    """

    employeeWindowSpec = Window.partitionBy("employee_number").orderBy(F.col("snapshot_date"))
    constWindowSpec = Window.partitionBy(F.lit(1)).orderBy(F.col("snapshot_date").desc())

    helper_columns = ["min_snapshot","row_hash","max_snapshot","prev_row_hash","next_row_hash","global_max_shapshot"]
    
    add_change_status_df = df \
        .withColumn("min_snapshot", F.min("snapshot_date").over(employeeWindowSpec)) \
        .withColumn("max_snapshot", F.max("snapshot_date").over(employeeWindowSpec)) \
        .withColumn("prev_row_hash", F.lag("row_hash").over(employeeWindowSpec)) \
        .withColumn("next_row_hash", F.lead("row_hash").over(employeeWindowSpec)) \
        .withColumn("global_max_shapshot", F.max("snapshot_date").over(constWindowSpec)) \
        .withColumn("change_status", 
                F.when(F.col("min_snapshot") == F.col("snapshot_date"), "New") \
                .when(F.col("next_row_hash").isNull() & (F.col("max_snapshot") != F.col("global_max_shapshot"))  , "Deleted") \
                .when(F.col("prev_row_hash") != F.col("row_hash"), "Changed") \
                .when(F.col("prev_row_hash") == F.col("row_hash"), "No Change") \
                .otherwise("Unknown") \
               ) \

    rowNum1Window = Window.partitionBy(["employee_number"]).orderBy(F.col("snapshot_date").desc())
    rowNum2Window = Window.partitionBy(["employee_number","row_hash"]).orderBy(F.col("snapshot_date").desc())
    gapGrpWindow = Window.partitionBy(["gap_grp"]).orderBy(F.col("snapshot_date"))

    helper_columns.extend(["gap_row_num_1","gap_row_num_2","gap_grp"])

    add_change_date_df = add_change_status_df \
        .withColumn("gap_row_num_1", F.row_number().over(rowNum1Window)) \
        .withColumn("gap_row_num_2", F.row_number().over(rowNum2Window)) \
        .withColumn("gap_grp", F.col("gap_row_num_1") - F.col("gap_row_num_2")) \
        .withColumn("changed_status_date",F.min("snapshot_date").over(gapGrpWindow)) \
        .withColumn("changed_status_date", \
                    F.when(F.col("change_status") == "Deleted",F.col("snapshot_date")) \
                    .otherwise(F.col("changed_status_date"))) \
        .drop(*helper_columns)
        
    
    return add_change_date_df




def test_DF(df,tp_dict):
    """Runs tests on df based on table profile dictionary data.
    :param df: DataFrame to print.
    :parama tp_dict: a table profile dictionary.
    :return: none 
    """
    
    dup_count = df \
            .groupBy(tp_dict["keys"]) \
            .count() \
            .filter(F.col("count") > 1) \
            .count()

    if df.schema != tp_dict["schema"]:
        raise CustomError("Test Fail: Schemas don't match")
    elif df.count() == 0:
        raise CustomError("Test Fail: Zero Rows")
    elif dup_count > 0:
        raise CustomError("Test Fail: Key Test Fail")
    else:
        pass

    return None

def write_DF(df, tp_dict):
    """Collects data locally and write to CSV based on path set in table profile dictionary.
    :param df: DataFrame to print.
    :parama tp_dict: a table profile dictionary.
    :return df: 
    """
    test_DF(df, tp_dict)

    df \
        .coalesce(1) \
        .write.format("com.databricks.spark.csv") \
        .mode('overwrite') \
        .option("nullValue", "NULL") \
        .option("header", "true") \
        .save(tp_dict["output_path"])

def move_files(input_file_list, tp_dict):
    """Moves files to new location based on table profile dictionary output_path.
    :param df: DataFrame to print.
    :parama tp_dict: a table profile dictionary.
    :return df: 
    """

    destination_path = tp_dict["output_path"]

    input_files = input_file_list
    
    for in_file_path in input_files:
        file_name = path.basename(in_file_path)
        
        out_file_path = os.path.join(destination_path,file_name)
           
        shutil.move(in_file_path, out_file_path)


#start Spark and register application
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Employee_Dim") \
    .getOrCreate()

#load table profiles
tp = config.table_profiles

#EXTRACT

#extract employee all table
emp_all_raw_DF, emp_all_file_list = load_csv_df(tp["employee_all"])

#extract raw employee snapshots and related imported file list
input_DF, input_file_list = load_csv_df(tp["emp_snapshots"])

#TRANSFORM
#create employee_all staging
emp_all_stage_DF = emp_all_raw_DF \
                .transform(import_new_files) \
                .transform(remove_columns) \
                .transform(add_row_hash) \
                .transform(get_change_status)

#create employee_current staging
emp_current_stage_DF = emp_all_stage_DF \
                    .transform(get_latest_snapshot)

#LOAD
write_DF(emp_all_stage_DF, tp["employee_all"])
write_DF(emp_current_stage_DF, tp["employee_current"])

#CLEAN UP
move_files(input_file_list,tp["emp_snapshots"])