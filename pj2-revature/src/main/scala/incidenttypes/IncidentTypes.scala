package incidenttypes

import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{count, desc, explode, lit, regexp_extract}

object IncidentTypes {

  def GetDataFrame(spark : SparkSession, path : String) : sql.DataFrame = {
    spark.read.option("multiline", "true").json(path)
  }

  def GetFilteredIncidentTypes(spark : SparkSession, path : String) : Dataset[Row] = {
    import spark.implicits._

    val df = GetDataFrame(spark, path)

    val incidentTypes = df.withColumn("data", explode($"data")).select( $"data.text")
      .as[TweetText]
      .map(tweet => {
        tweet.text match {
          case text if text.matches(".*?\\bAccident\\scleared\\b.*?") => {"Not Relevant"}
          case text if text.matches(".*?\\bAccident\\b.*?") => {"Accident"}
          case text if text.matches(".*?\\bRamp\\srestrictions\\b.*?") => {"Ramp Restriction"}
          case text if text.matches(".*?\\bOff-ramp\\sblocked\\b.*?") => {"Off-Ramp Closed"}
          case text if text.matches(".*?\\bOff-ramp\\sclosed\\b.*?") => {"Off-Ramp Closed"}
          case text if text.matches(".*?\\bOn-ramp\\sclosed\\b.*?") => {"On-Ramp Closed"}
          case text if text.matches(".*?\\bBridge\\sclosed\\b.*?") => {"Bridge Closed"}
          case text if text.matches(".*?\\bDisabled\\b.*?") => {"Disabled Vehicle"}
          case text if text.matches(".*?\\bStall\\b.*?") => {"Disabled Vehicle"}
          case text if text.matches(".*?\\bA\\sstalled\\b.*?") => {"Disabled Vehicle"}
          case text if text.matches(".*?\\bA\\sstall\\b.*?") => {"Disabled Vehicle"}
          case text if text.matches(".*?\\bStalled\\b.*?") => {"Disabled Vehicle"}
          case text if text.matches(".*?\\bOverturned\\svehicle\\b.*?") => {"Accident"}
          case text if text.matches(".*?\\bClosed\\sdue\\sto\\saccident\\b.*?") => {"Accident"}
          case text if text.matches(".*?\\bRoad\\sclosed\\sdue\\sto\\b.*?") => {"Road Closed"}
//          case text if text.matches(".*?\\bClosed\\sdue\\sto\\b.*?") => {"Road Closed"}
          case text if text.matches(".*?\\bThe\\sroad\\sis\\sclosed\\b.*?") => {"Road Closed"}
//          case text if text.matches(".*?\\bClosed\\sin\\b.*?") => {"Road Closed"}
//          case text if text.matches(".*?\\bClosed\\sfor\\b.*?") => {"Road Closed"}
          case text if text.matches(".*?\\bClosed\\b.*?") => {"Road Closed"}
          case text if text.matches(".*?\\bBlocked\\sdue\\sto\\b.*?") => {"Road Blocked"}
          case text if text.matches(".*?\\bRoad\\sblocked\\sdue\\sto\\b.*?") => {"Road Blocked"}
          case text if text.matches(".*?\\bRoad\\sconstruction\\b.*?") => {"Construction"}
          case text if text.matches(".*?\\bConstruction\\b.*?") => {"Construction"}
          case text if text.matches(".*?\\bBrush\\sfire\\b.*?") => {"Brush Fire"}
          case notFound => "Not Relevant"
        }
      })
      .withColumnRenamed("value", "incident_type")
      // Exclude Accidents and Irrelevant Data
      .filter($"incident_type" =!= "Not Relevant" && $"incident_type" =!= "Accident")

    incidentTypes
  }

  def GetIncidentTypesTotal(spark : SparkSession, path : String) : Dataset[Row] = {
    GetFilteredIncidentTypes(spark, path)
      .groupBy("incident_type")
      .agg(count("incident_type") as "incident_occ")
      .orderBy(desc("incident_occ"))
  }

  // Returns total incidents from GetIncidentTypesTotal along with an AVG based on 2 days for comparison
  def GetWeekendIncidentTypesTotalsWithAvg(spark : SparkSession, path : String) : Dataset[Row] = {
    GetFilteredIncidentTypes(spark, path)
      .groupBy("incident_type")
      .agg(
        count("incident_type") as "incident_occ",
        count("incident_type") / 2 as "incident_occ_avg"
      )
      .orderBy(desc("incident_occ_avg"))
  }

  // Returns total incidents from GetIncidentTypesTotal along with an AVG based on 5 days for comparison
  def GetWeekdaysIncidentTypesTotalsWithAvg(spark : SparkSession, path : String) : Dataset[Row] = {
    GetFilteredIncidentTypes(spark, path)
      .groupBy("incident_type")
      .agg(
        count("incident_type") as "incident_occ",
        count("incident_type") / 5 as "incident_occ_avg"
      )
      .orderBy(desc("incident_occ_avg"))
  }

  def GetRushHourIncidents(spark : SparkSession, path : String) : Dataset[Row] = {
    import spark.implicits._

    val df = GetDataFrame(spark, path)

    val incidentTypes = df.withColumn("data", explode($"data"))
      .select( $"data.id", $"data.created_at")

    val df_general_extract = incidentTypes
      .select($"id", $"created_at")
      .withColumn("Date", regexp_extract(incidentTypes("created_at"),"([0-9]+\\W[0-9][0-9]+\\W[0-9][0-9])(.*T)", 1))
      .withColumn("Time", regexp_extract(incidentTypes("created_at"),"(T)([0-9]+\\W[0-9][0-9]+\\W[0-9][0-9])(.*Z)", 2))
      .select($"id", functions.concat($"Date", lit(" "), $"Time").alias("date_time_accident"))

    df_general_extract.createOrReplaceTempView("general_extract")

    val unix_timestamp_conversion_query = "SELECT id, " +
    "UNIX_TIMESTAMP(date_time_accident) AS time_occ " +
    "FROM general_extract" +
    ";"

    spark.sql(unix_timestamp_conversion_query).createOrReplaceTempView("inc_unix_time")

    val dfAllIncidents = df.withColumn("data", explode($"data"))
      .select( $"data.id", $"data.text")

    dfAllIncidents.createOrReplaceTempView("all_accidents")

    val mergedIncidents = "SELECT all_accidents.text " +
    "FROM all_accidents " +
    "INNER JOIN inc_unix_time ON all_accidents.id = inc_unix_time.id " +
    "WHERE (MOD(inc_unix_time.time_occ, (24 * 3600)) > 82800 AND MOD(inc_unix_time.time_occ, (24 * 3600)) < 7200) " +
    "OR (MOD(inc_unix_time.time_occ, (24 * 3600)) > 28800 AND MOD(inc_unix_time.time_occ, (24 * 3600)) < 39600)"
    ";"

    spark.sql(mergedIncidents).as[TweetText]
      .map(tweet => {
        tweet.text match {
          case text if text.matches(".*?\\bAccident\\scleared\\b.*?") => {"Not Relevant"}
          case text if text.matches(".*?\\bAccident\\b.*?") => {"Accident"}
          case text if text.matches(".*?\\bRamp\\srestrictions\\b.*?") => {"Ramp Restriction"}
          case text if text.matches(".*?\\bOff-ramp\\sblocked\\b.*?") => {"Off-Ramp Closed"}
          case text if text.matches(".*?\\bOff-ramp\\sclosed\\b.*?") => {"Off-Ramp Closed"}
          case text if text.matches(".*?\\bOn-ramp\\sclosed\\b.*?") => {"On-Ramp Closed"}
          case text if text.matches(".*?\\bBridge\\sclosed\\b.*?") => {"Bridge Closed"}
          case text if text.matches(".*?\\bDisabled\\b.*?") => {"Disabled Vehicle"}
          case text if text.matches(".*?\\bStall\\b.*?") => {"Disabled Vehicle"}
          case text if text.matches(".*?\\bA\\sstalled\\b.*?") => {"Disabled Vehicle"}
          case text if text.matches(".*?\\bA\\sstall\\b.*?") => {"Disabled Vehicle"}
          case text if text.matches(".*?\\bStalled\\b.*?") => {"Disabled Vehicle"}
          case text if text.matches(".*?\\bOverturned\\svehicle\\b.*?") => {"Accident"}
          case text if text.matches(".*?\\bClosed\\sdue\\sto\\saccident\\b.*?") => {"Accident"}
          case text if text.matches(".*?\\bRoad\\sclosed\\sdue\\sto\\b.*?") => {"Road Closed"}
          case text if text.matches(".*?\\bClosed\\sdue\\sto\\b.*?") => {"Road Closed"}
          case text if text.matches(".*?\\bThe\\sroad\\sis\\sclosed\\b.*?") => {"Road Closed"}
          case text if text.matches(".*?\\bClosed\\sin\\b.*?") => {"Road Closed"}
          case text if text.matches(".*?\\bClosed\\sfor\\b.*?") => {"Road Closed"}
          case text if text.matches(".*?\\bClosed\\b.*?") => {"Road Closed"}
          case text if text.matches(".*?\\bBlocked\\sdue\\sto\\b.*?") => {"Road Blocked"}
          case text if text.matches(".*?\\bRoad\\sblocked\\sdue\\sto\\b.*?") => {"Road Blocked"}
          case text if text.matches(".*?\\bRoad\\sconstruction\\b.*?") => {"Construction"}
          case text if text.matches(".*?\\bConstruction\\b.*?") => {"Construction"}
          case text if text.matches(".*?\\bBrush\\sfire\\b.*?") => {"Brush Fire"}
          case notFound => "Not Relevant"
        }
      })
      .withColumnRenamed("value", "incident_type")
      // Exclude Accidents and Irrelevant Data
      .filter($"incident_type" =!= "Not Relevant" && $"incident_type" =!= "Accident")
      .groupBy("incident_type")
      .agg(
        count("incident_type") as "incident_occ",
        count("incident_type") / 5 as "incident_occ_avg"
      )
      .orderBy(desc("incident_occ_avg"))
  }

  case class TweetText(text: String)

}
