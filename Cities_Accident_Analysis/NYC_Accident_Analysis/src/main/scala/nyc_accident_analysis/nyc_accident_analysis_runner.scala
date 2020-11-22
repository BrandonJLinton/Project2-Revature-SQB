package nyc_accident_analysis
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, flatten}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.SaveMode

object nyc_accident_analysis_runner {

  def main(args: Array[String]):Unit= {

    //Local testing
    /*
    val spark = SparkSession.builder() // session builder the gateway to spark-SQL
      .appName("NYC_Accident_Analysis")
      .master("local[4]")
      .getOrCreate()

      val NYC_File_Location="NYC-FullWeek.json"
     */

    val spark = SparkSession.builder() // session builder the gateway to spark-SQL
      .appName("NYC_Accident_Analysis")
      .getOrCreate()

    val NYC_File_Location = "s3://rev-big-data/NYC-FullWeek.json"

    import spark.implicits._ //to enable the use of dollar signs
    spark.sparkContext.setLogLevel("WARN") // avoid unnecessary readouts

    val df = spark.read.option("multiline","true").json(NYC_File_Location) //read json data

    val df_un_nest=df.select(explode(df("data"))) // remove nested arrays

    //df_General_data is for extracting columns I need for analysis
    val df_General_data = df_un_nest.select(df_un_nest("col.id").alias("Id"),
      df_un_nest("col.entities.hashtags.tag").getItem(0)
        .alias("General_Location"),
      df_un_nest("col.entities.annotations.normalized_text").getItem(0)
        .alias("Incident_Occurrence_Rd"),
      df_un_nest("col.entities.annotations.normalized_text").getItem(1)
        .alias("Detailed_Location"),
      df_un_nest("col.text").alias("Description"),
      df_un_nest("col.created_at").alias("DateTime_Of_Incident"))

    // to query for accidents
    df_General_data.createTempView("General_data")

    //Querying the description of tweet to get only accidents
    val General_Accident_Query = "SELECT Id, General_Location, " +
      "Incident_Occurrence_Rd AS Accident_Occurrence_Rd, " +
      "Detailed_Location AS Detailed_Accident_Location, " +
      "DateTime_Of_Incident AS Date_Time_Of_Accident  " +
      "FROM General_data  " +
      "WHERE Description NOT LIKE '%Accident_cleared%' " +
      "AND Description LIKE '%Accident%' " +
      "OR Description LIKE '%Closed_due_to_accident%' " +
      "OR Description LIKE '%Overturned_vehicle%' " +
      ";"

    val df_General_Accident_data=spark.sql(General_Accident_Query) // Only accidents



    //inner-join with unix_timestamp table
    df_General_Accident_data.createTempView("General_Accident_Data")

    //extracting only Id and Time_Of_Accident to extract Date and Time for Unix_timestamp conversion
    val df_General_Accident_extract_data_time=df_General_Accident_data.select(df_General_Accident_data("Id"),
      df_General_Accident_data("Date_Time_Of_Accident"))
      .withColumn("Date",regexp_extract(df_General_Accident_data("Date_Time_Of_Accident"),
        "([0-9]+\\W[0-9][0-9]+\\W[0-9][0-9])(.*T)",1))
      .withColumn("Time",regexp_extract(df_General_Accident_data("Date_Time_Of_Accident"),
        "(T)([0-9]+\\W[0-9][0-9]+\\W[0-9][0-9])(.*Z)",2))
      .select(df_General_Accident_data("Id"),$"Date",$"Time",
        concat($"Date", lit(" "), $"Time").alias("Date_Time_Of_Accident"))


    //temp view for converting date and time to unix_timestamp
    df_General_Accident_extract_data_time.createTempView("General_Accident_Extract_Date_Time")

    //query for converting data and time to unix_timestamp
    val unix_timestamp_conversion_query="SELECT Id, Date, Time, " +
      "(UNIX_TIMESTAMP(Date_Time_Of_Accident)-18000) AS Date_Time_Of_Accident " +
      "FROM General_Accident_Extract_Date_Time "+
      ";"

    //saving result to another dataframe to inner join with General_Accident_Data
    val df_Accident_Id_Date_Time=spark.sql(unix_timestamp_conversion_query)


    //inner join with Accident_General_data
    df_Accident_Id_Date_Time.createTempView("Accident_Data_Id_Date_Time")

    //performing inner join with General_Accident_Table to get unix_timestamp added to table

    val General_Accident_with_Unix_Timestamp_query="SELECT GAD.Id, GAD.General_Location, " +
      "GAD.Accident_Occurrence_Rd, GAD.Detailed_Accident_Location, " +
      "AIDT.Date, AIDT.Time, " +
      "AIDT.Date_Time_Of_Accident AS Unix_TimeStamp_Of_Accident " +
      "FROM General_Accident_Data GAD "+
      "INNER JOIN "+
      "Accident_Data_Id_Date_Time AIDT "+
      "ON GAD.Id = AIDT.Id "

    //saving the result of accidents with unix_timestamp
    val df_General_Accident_unix_timestamp=spark.sql(General_Accident_with_Unix_Timestamp_query)


    //tempview of to eventually find distinct accidents
    df_General_Accident_unix_timestamp.createTempView("General_Accidents_UT_Table")

    //using grouping sets to get distinct accidents
    val grouping_set_Query="SELECT General_Location, Accident_Occurrence_Rd,  "+
      "Detailed_Accident_Location, MIN(Unix_TimeStamp_Of_Accident) AS Date_Time_Of_Accident "+
      "FROM General_Accidents_UT_Table  "+
      "GROUP BY General_Location, Accident_Occurrence_Rd, Detailed_Accident_Location "+
      ";"


    //THE DISTINCT ACCIDENT TABLE!!!!
    val df_Distinct_Accidents_Similar_Accidents_Table=spark.sql(grouping_set_Query)

    df_Distinct_Accidents_Similar_Accidents_Table.createTempView("Distinct_Accidents_Similar_Names_Table")

    val Group_Same_General_Location_Name_Distinct_Accident_Table_query="SELECT "+
      "CASE " +
      "WHEN General_Location LIKE '%Bronx%' THEN 'Bronx' " +
      "WHEN General_Location LIKE '%Brooklyn%' THEN 'Brooklyn' " +
      "WHEN General_Location LIKE 'Nyc' THEN 'Manhattan' " +
      "WHEN General_Location LIKE '%StatenIsland%' THEN 'StatenIsland' " +
      "ELSE General_Location " +
      "END AS General_Location, " +
      "Accident_Occurrence_Rd, " +
      "Detailed_Accident_Location, " +
      "Date_Time_Of_Accident " +
      "FROM Distinct_Accidents_Similar_Names_Table " +
      ";"

    val df_Distinct_Accident_Table=spark.sql(Group_Same_General_Location_Name_Distinct_Accident_Table_query)

    df_Distinct_Accident_Table.createTempView("Distinct_Accidents_Table")

    //The General Location of where most accidents occur (Timeline: Entire Time)
    val General_Location_Query_ET= "SELECT " +
      "General_Location, " +
      "COUNT(*) AS FULL_WEEK_COUNT "+
      "FROM Distinct_Accidents_Table "+
      "WHERE Date_Time_Of_Accident BETWEEN 1604880000 AND 1605484740 "+
      "GROUP BY  "+
      "General_Location " +
      "ORDER BY FULL_WEEK_COUNT DESC, General_Location "+
      ";"

    spark.sql(General_Location_Query_ET).show(5,false)

    //The General Location of where most accidents occur (Timeline: Weekdays)
    val General_Location_Query_WD="SELECT " +
      "General_Location, " +
      "COUNT(*) AS WEEKDAY_COUNT "+
      "FROM Distinct_Accidents_Table "+
      "WHERE Date_Time_Of_Accident BETWEEN 1604880000 AND 1605311940 "+
      "GROUP BY  "+
      "General_Location " +
      "ORDER BY WEEKDAY_COUNT DESC, General_Location "+
      ";"
    spark.sql(General_Location_Query_WD).show(5,false)

    //The General Location of where most accidents occur (Timeline: Weekend)
    val General_Location_Query_WND="SELECT " +
      "General_Location, " +
      "COUNT(*) AS WEEKEND_COUNT "+
      "FROM Distinct_Accidents_Table "+
      "WHERE Date_Time_Of_Accident BETWEEN 1605312000 AND 1605484740 "+
      "GROUP BY  "+
      "General_Location " +
      "ORDER BY WEEKEND_COUNT DESC, General_Location "+
      ";"
    spark.sql(General_Location_Query_WND).show(5,false)

    //The General Location of where most accidents occur (Timeline: Rush-Hour)
    val General_Location_Query_RH="SELECT " +
      "General_Location, " +
      "COUNT(*) AS RUSH_HOUR_COUNT "+
      "FROM Distinct_Accidents_Table "+
      "WHERE Date_Time_Of_Accident BETWEEN 1604905200 AND (1604905200+10800)  " +
      "OR Date_Time_Of_Accident BETWEEN (1604905200+32400) AND (1604905200+43200)  " +
      "OR Date_Time_Of_Accident BETWEEN 1604991600 AND (1604991600+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1604991600+32400) AND (1604991600+43200)  " +
      "OR Date_Time_Of_Accident BETWEEN 1605078000 AND (1605078000+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1605078000+32400) AND (1605078000+43200) " +
      "OR Date_Time_Of_Accident BETWEEN 1605164400 AND (1605164400+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1605164400+32400) AND (1605164400+43200) " +
      "OR Date_Time_Of_Accident BETWEEN 1605250800 AND (1605250800+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1605250800+32400) AND (1605250800+43200) " +
      "GROUP BY  "+
      "General_Location " +
      "ORDER BY RUSH_HOUR_COUNT DESC, General_Location "+
      ";"
    spark.sql(General_Location_Query_RH).show(5,false)

    // The Road where most accidents occur (Timeline: Entire-Time)
    val General_Road_Location_Query_ET= "SELECT " +
      "General_Location,  " +
      "Accident_Occurrence_Rd,  " +
      "COUNT(*) AS FULL_WEEK_COUNT "+
      "FROM Distinct_Accidents_Table "+
      "WHERE Date_Time_Of_Accident BETWEEN 1604880000 AND 1605484740 " +
      "AND Accident_Occurrence_Rd IS NOT NULL "+
      "GROUP BY  "+
      "General_Location, " +
      "Accident_Occurrence_Rd  "+
      "ORDER BY FULL_WEEK_COUNT DESC, General_Location, Accident_Occurrence_Rd "+
      ";"

    spark.sql(General_Road_Location_Query_ET).show(5,false)

    //The Road where most accidents occur (Timeline: Weekdays)
    val General_Road_Location_Query_WD="SELECT " +
      "General_Location, " +
      "Accident_Occurrence_Rd,  " +
      "COUNT(*) AS WEEKDAY_COUNT "+
      "FROM Distinct_Accidents_Table "+
      "WHERE Date_Time_Of_Accident BETWEEN 1604880000 AND 1605311940 " +
      "AND Accident_Occurrence_Rd IS NOT NULL  "+
      "GROUP BY  "+
      "General_Location, " +
      "Accident_Occurrence_Rd "+
      "ORDER BY WEEKDAY_COUNT DESC, General_Location, Accident_Occurrence_Rd "+
      ";"

    spark.sql(General_Road_Location_Query_WD).show(5,false)

    //The Road where most accidents occur (Timeline: Weekends)
    val General_Road_Location_Query_WND="SELECT " +
      "General_Location, " +
      "Accident_Occurrence_Rd, " +
      "COUNT(*) AS WEEKEND_COUNT "+
      "FROM Distinct_Accidents_Table "+
      "WHERE Date_Time_Of_Accident BETWEEN 1605312000 AND 1605484740 " +
      "AND Accident_Occurrence_Rd IS NOT NULL "+
      "GROUP BY  "+
      "General_Location, " +
      "Accident_Occurrence_Rd  "+
      "ORDER BY WEEKEND_COUNT DESC, General_Location, Accident_Occurrence_Rd "+
      ";"

    spark.sql(General_Road_Location_Query_WND).show(5,false)

    //The Road where most accidents occur (Timeline: Rush-Hour)
    val General_Road_Location_Query_RH="SELECT " +
      "General_Location, " +
      "Accident_Occurrence_Rd, " +
      "COUNT(*) AS RUSH_HOUR_COUNT "+
      "FROM Distinct_Accidents_Table "+
      "WHERE Date_Time_Of_Accident BETWEEN 1604905200 AND (1604905200+10800)  " +
      "OR Date_Time_Of_Accident BETWEEN (1604905200+32400) AND (1604905200+43200)  " +
      "OR Date_Time_Of_Accident BETWEEN 1604991600 AND (1604991600+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1604991600+32400) AND (1604991600+43200)  " +
      "OR Date_Time_Of_Accident BETWEEN 1605078000 AND (1605078000+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1605078000+32400) AND (1605078000+43200) " +
      "OR Date_Time_Of_Accident BETWEEN 1605164400 AND (1605164400+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1605164400+32400) AND (1605164400+43200) " +
      "OR Date_Time_Of_Accident BETWEEN 1605250800 AND (1605250800+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1605250800+32400) AND (1605250800+43200) " +
      "AND Accident_Occurrence_Rd IS NOT NULL " +
      "GROUP BY  "+
      "General_Location, " +
      "Accident_Occurrence_Rd "+
      "ORDER BY RUSH_HOUR_COUNT DESC, General_Location, Accident_Occurrence_Rd "+
      ";"

    spark.sql(General_Road_Location_Query_RH).show(5,false)

    //Total Accidents for NYC (Timeline: Entire Time)
    val counting_query_ET="SELECT COUNT(*) AS NYC_ENTIRE_TIME_ACCIDENT_COUNTS, " +
      "(COUNT(*)/7) AS NYC_ENTIRE_TIME_ACCIDENT_AVERAGE  "+
      "FROM Distinct_Accidents_Table " +
      "WHERE Date_Time_Of_Accident BETWEEN 1604880000 AND 1605484740 "+
      ";"

    spark.sql(counting_query_ET).show(false)

    //Total Accidents for NYC (Timeline: Weekdays)
    val counting_query_WD="SELECT COUNT(*) AS NYC_WEEKDAY_ACCIDENT_COUNTS, " +
      "(COUNT(*)/5) AS NYC_WEEKDAY_ACCIDENT_AVERAGE  "+
      "FROM Distinct_Accidents_Table  " +
      "WHERE Date_Time_Of_Accident BETWEEN 1604880000 AND 1605311940 "+
      ";"

     spark.sql(counting_query_WD).show(false)

    //Total Accidents for NYC (Timeline: Weekends)
    val counting_query_WND="SELECT COUNT(*) AS NYC_WEEKENDS_ACCIDENT_COUNTS, " +
      "(COUNT(*)/2) AS NYC_WEEKENDS_ACCIDENT_AVERAGE  "+
      "FROM Distinct_Accidents_Table  " +
      "WHERE Date_Time_Of_Accident BETWEEN 1605312000 AND 1605484740 "+
      ";"

     spark.sql(counting_query_WND).show(false)

    //Total Accidents for NYC (Timeline: Rush-Hour)
    val counting_query_RH="SELECT COUNT(*) AS NYC_RUSH_HOUR_ACCIDENT_COUNTS, " +
      "(COUNT(*)/10) AS NYC_RUSH_HOUR_ACCIDENT_AVERAGE  "+
      "FROM Distinct_Accidents_Table  " +
      "WHERE Date_Time_Of_Accident BETWEEN 1604905200 AND (1604905200+10800) "+
      "OR Date_Time_Of_Accident BETWEEN (1604905200+32400) AND (1604905200+43200)  " +
      "OR Date_Time_Of_Accident BETWEEN 1604991600 AND (1604991600+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1604991600+32400) AND (1604991600+43200)  " +
      "OR Date_Time_Of_Accident BETWEEN 1605078000 AND (1605078000+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1605078000+32400) AND (1605078000+43200) " +
      "OR Date_Time_Of_Accident BETWEEN 1605164400 AND (1605164400+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1605164400+32400) AND (1605164400+43200) " +
      "OR Date_Time_Of_Accident BETWEEN 1605250800 AND (1605250800+10800) " +
      "OR Date_Time_Of_Accident BETWEEN (1605250800+32400) AND (1605250800+43200) " +
      ";"

     spark.sql(counting_query_RH).show(false)




  }

}
