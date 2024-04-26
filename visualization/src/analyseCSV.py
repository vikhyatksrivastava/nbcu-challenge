from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import os
import shutil

#Setup SparkSession
def setupSpark():
    appName = "messageAnalyser"
    master = "local"
    spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
    return spark

#Schema Definition of the hive table
def inferSchema():
    schema = StructType([
        StructField('id', StringType(), True),
        StructField('item', StringType(), True),
        StructField('rating', IntegerType(), True),
        StructField('datetime', DateType(), True)
    ])
    return schema

#Reading the csv file and storing it into dataframe
def processCSVtoDataframeAndTable(filename):
    schema = inferSchema()
    spark=setupSpark()
    datafile = spark.read.csv(filename, header=True, schema=schema)
    datafile = datafile.withColumn("datetime",to_date(col("datetime"),"yyyy-MM-dd"))
    datafile.write.mode("overwrite").saveAsTable("Source_Message_Table")
    datafile.coalesce(1).write.option("header","true").csv('../peacock-de-eval.tar/output/Source_Message_Data','overwrite')
    fetchAndRenameFile('../peacock-de-eval.tar/output/Source_Message_Data/',
                       '../peacock-de-eval.tar/output/Source_Message_Data.csv')
    shutil.rmtree('../peacock-de-eval.tar/output/Source_Message_Data/')
    return datafile

#Rename the part files to meaningful csv filename for further use
def fetchAndRename(output_path, outputFilename):
    output_file_name = output_path.split("/")[-1]
    os.rename(output_file_name, outputFilename)

#Fetching most popular titles by count of items wrt rating in descending order and saving to a table
def getAnalytics(fileName):
    datafile = processCSVtoDataframeAndTable(fileName)

    #logic for most popular titles
    groupedItemDf = datafile.groupBy("item","rating").agg(count("*").alias("count"))
    orderedItemDf = groupedItemDf.where("rating>0").orderBy(col("rating").desc(),col("count").desc())
    orderedItemDf.write.mode("overwrite").saveAsTable("Most_Popular_Titles")
    orderedItemDf.coalesce(1).write.option("header", "true").csv('../peacock-de-eval.tar/output/Most_Popular_Titles',
                                                            'overwrite')
    fetchAndRenameFile('../peacock-de-eval.tar/output/Most_Popular_Titles/', '../peacock-de-eval.tar/output/Most_Popular_Titles.csv')
    shutil.rmtree('../peacock-de-eval.tar/output/Most_Popular_Titles/')

    #considering patrons are ids, below logic populates data to show the count ids by rating
    groupedIdDf = datafile.groupBy("id", "rating").agg(count("*").alias("count"))
    orderedIdDf = groupedIdDf.where("rating>0").orderBy(col("id").desc(), col("count").desc())
    orderedIdDf.write.mode("overwrite").saveAsTable("Id_Counts_By_Rating")
    orderedIdDf.coalesce(1).write.option("header", "true").csv('../peacock-de-eval.tar/output/Id_Counts_By_Rating',
                                                            'overwrite')
    fetchAndRenameFile('../peacock-de-eval.tar/output/Id_Counts_By_Rating/',
                       '../peacock-de-eval.tar/output/Id_Counts_By_Rating.csv')
    shutil.rmtree('../peacock-de-eval.tar/output/Id_Counts_By_Rating/')

    #below logic gives the number of rating given to the items on each day
    groupedDtDf = datafile.groupBy("datetime").agg(count("*").alias("count"))
    orderedDtDf = groupedDtDf.orderBy(col("datetime").desc())
    orderedDtDf.write.mode("overwrite").saveAsTable("Daily_Count_of_Rating")
    orderedDtDf.coalesce(1).write.option("header", "true").csv('../peacock-de-eval.tar/output/Daily_Count_of_Rating',
                                                            'overwrite')
    fetchAndRenameFile('../peacock-de-eval.tar/output/Daily_Count_of_Rating/',
                       '../peacock-de-eval.tar/output/Daily_Count_of_Rating.csv')
    shutil.rmtree('../peacock-de-eval.tar/output/Daily_Count_of_Rating/')

    #below logic gives the number of items rated on each day
    groupedItemDtDf = datafile.groupBy("item","datetime").agg(count("*").alias("count"))
    orderedItemDtDf = groupedItemDtDf.orderBy(col("item").desc(),col("datetime").desc())
    orderedItemDtDf.show(10)
    orderedItemDtDf.write.mode("overwrite").saveAsTable("Daily_Item_Count_Rated")
    orderedItemDtDf.coalesce(1).write.option("header", "true").csv('../peacock-de-eval.tar/output/Daily_Item_Count_Rated',
                                                            'overwrite')
    fetchAndRenameFile('../peacock-de-eval.tar/output/Daily_Item_Count_Rated/',
                       '../peacock-de-eval.tar/output/Daily_Item_Count_Rated.csv')
    shutil.rmtree('../peacock-de-eval.tar/output/Daily_Item_Count_Rated/')

def fetchAndRenameFile(partFilePath, outputFilePath):
    if os.path.exists(outputFilePath):
        # If the file exists, remove it
        os.remove(outputFilePath)

    files = os.listdir(partFilePath)

    part_files = [file for file in files if (file.startswith("part-") and file.endswith("-c000.csv"))]

    for part_file in part_files:
        print(part_file)
        partFileAbsPath = partFilePath+part_file
        os.rename(partFileAbsPath,outputFilePath)

def main():
    fileName = "../peacock-de-eval.tar/output/messages.csv"
    getAnalytics(fileName)

if __name__ == '__main__':
    main()