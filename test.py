'''
Created on Mar 17, 2019

@author: goyalg1
''' 
import pyspark
from pyspark import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
import re
from pyspark.sql.types import StringType
from py4j.protocol import Py4JJavaError
from datetime import datetime
from os import walk
from pyspark.sql import dataframe
#Create a class Recipe with initializing the entire JSON document to data frame
#Also two functions responsible for converting the JSON file into Parquet format along with capturing the changed data only
class Recipe:
  def __init__(self, my_dataframe):
      self.reciepy_dataframe = my_dataframe
  #This function is responsible for text 'BEEF' inside the spark dataframe
  #Although i would use the SPARKSQL to figure out the same, that's why i didn't made the call to this function. 
  #As doing this will be an expensive affair.
  
  def search_in_col(self,searchFor,column,changeDF):    
    if (changeDF):
        df = self.reciepy_dataframe.where(F.col(column).rlike(searchFor))
        return self.reciepy_dataframe
    return self.reciepy_dataframe.where(F.col(column).rlike(searchFor))
  #Function creates a new Parquet output file if no output exists yet.
  #If output exists, it appends only the changed data to that output
  def store_data(self,path):
    try:
        df_old = spark.read.parquet(path)
        df = df_old.union(self.reciepy_dataframe.join(df_old, self.reciepy_dataframe.cols, how ='left_anti').withColumn('ts', lit(datetime.now())))
        df.write.parquet(
        path=path,
        mode="overwrite",
        compression="snappy")
                
    except:
        df = self.reciepy_dataframe.withColumn('ts', lit(datetime.now()))
        df.write.parquet(
        path=path,
        mode="overwrite",
        compression="snappy")

    
#Find the string with Hour/Minutes with the passed values and calculate the actual time in Minutes     
def find_mins(val):
    matches = re.findall('\d+\w', val)
    mins = 0

    if (matches):
        for match in matches:
            if (match[-1] == 'H'):
                mins += int(match[:-1]) * 60
            elif (match[-1] == 'M'):
                mins += int(match[:-1])
    else:
        mins = 0
    return mins

#To calculate difficulty, prepTime and cookTime values are parsed to ascertain number of minutes.
#If one of prepTime or cookTime is an empty string or invalid value, its value is considered zero.
#If both prepTime and cookTime are empty of invalid strings, 'Unknown' is set to 'difficulty' column  
def difficulty(pt, ct):
    t_mins = find_mins(pt) + find_mins(ct)
    if (t_mins > 0 and t_mins < 30):
        return 'Easy'
    elif (t_mins >= 30 and t_mins <= 60):
        return 'Medium'
    elif (t_mins > 60):
        return 'Hard'
    else:
        return 'Unknown'


    
sc = SparkContext()
spark = SparkSession(sc)
try:
    #Create the Recipe object which will hold the JSON data and all transformation will be done over that object only
    #Ideally we should have path parameter as input one rather than hard coded, we can do so if required
    #Just thought to have single bucket and in case that changes then it is going to impact the Parquet file creation
    r1 = Recipe(spark.read.json('s3a://codefile1/recipes.json', multiLine=True))

    #Alter the dataframe and introduce new column that will hold the 'Difficulty' parameter
    difficulty_udf = F.UserDefinedFunction(difficulty, StringType()) 
            
    r1.reciepy_dataframe = r1.reciepy_dataframe.withColumn('newCol', difficulty_udf(r1.reciepy_dataframe['prepTime'], r1.reciepy_dataframe['cookTime']))
    
    #Verify if the dataframe has the new column with correct data        
    r1.reciepy_dataframe.show(5)
            
    #Storing the data to parquet file over S3 bucket
    #Ideally we should have path parameter as input one rather than hard coded, we can do so if required
    #Just thought to have single bucket and in case that changes then it is going to impact the Parquet file creation
    r1.store_data('s3a://codefile1/fin.parquet')
            
    # for each parquet file, i.e. table in our database, spark creates a tempview with
    # the respective table name equal the parquet filename
    parquetFile = spark.read.parquet('s3a://codefile1/fin.parquet')
    parquetFile.createOrReplaceTempView("parquetFile")
    teenagers = spark.sql("SELECT * FROM parquetFile LIMIT 10")
    teenagers.show()
except Exception as e:
    print(str(e))
    
#print(r1.name)
#print(r1.ptime)





