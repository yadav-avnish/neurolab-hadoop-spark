"""
This is repo is created to demonstrate how data can be stored in hadoop using spark.
Provided code is create to run on neurolab of organization Ineuron Intelligence Private Limited
Created date: 20th-Oct-2022
Created by: Avnish Yadav

"""


#necessary libraries of pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType


if __name__ == '__main__':
    #Creating spark session
    spark = SparkSession.builder.master("spark://localhost:7077").appName("demo").getOrCreate()

    #Create list of data to prepare data frame
    person_list = [("Berry","","Allen","1","M"),
        ("Oliver","Queen","","2","M"),
        ("Robert","","Williams","3","M"),
        ("Tony","","Stark","4","F"),
        ("Rajiv","Mary","Kumar","5","F")
    ]
    

    #defining schema for dataset
    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
      
    ])
    
    #creating spark dataframe
    df = spark.createDataFrame(data=person_list,schema=schema)

    #Printing data frame schema
    df.printSchema()

    #Printing data
    df.show(truncate=False)

    #Writing file in hadoop
    df.write.csv("record.csv")
