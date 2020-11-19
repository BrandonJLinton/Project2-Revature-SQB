package traffictracker

import incidenttypes.IncidentTypes.{GetIncidentTypesTotal, GetRushHourIncidents, GetWeekdaysIncidentTypesTotalsWithAvg, GetWeekendIncidentTypesTotalsWithAvg}
import org.apache.spark.sql.SparkSession

object Runner {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Traffic Tracker")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("NYC Full Week - Non-Accidents:")
    // Display all Non-Accidents for a full week
    GetIncidentTypesTotal(spark, "NYC-FullWeek.json").show()
    println("NYC Weekdays - Non-Accidents:")
    // Display all Non-Accidents for weekdays
    GetWeekdaysIncidentTypesTotalsWithAvg(spark, "NYC-Weekdays.json").show()
    println("NYC Weekends - Non-Accidents:")
    // Display all Non-Accidents for weekends
    GetWeekendIncidentTypesTotalsWithAvg(spark, "NYC-Weekend.json").show()
    println("NYC Rush-Hour - Non-Accidents:")
    // Display all Non-Accidents during Rush-Hour (7am-10am, 4pm-7pm)
    GetRushHourIncidents(spark, "NYC-Weekdays.json").show()

    println("HOU Full Week - Non-Accidents:")
    // Display all Non-Accidents for a full week
    GetIncidentTypesTotal(spark, "HOU-FullWeek.json").show()
    println("HOU Weekdays - Non-Accidents:")
    // Display all Non-Accidents for weekdays
    GetWeekdaysIncidentTypesTotalsWithAvg(spark, "HOU-Weekdays.json").show()
    println("HOU Weekends - Non-Accidents:")
    // Display all Non-Accidents for weekends
    GetWeekendIncidentTypesTotalsWithAvg(spark, "HOU-Weekend.json").show()
    println("HOU Rush-Hour - Non-Accidents:")
    // Display all Non-Accidents during Rush-Hour (7am-10am, 4pm-7pm)
    GetRushHourIncidents(spark, "HOU-Weekdays.json").show()

    println("PHL Full Week - Non-Accidents:")
    // Display all Non-Accidents for a full week
    GetIncidentTypesTotal(spark, "PHL-FullWeek.json").show()
    println("PHL Weekdays - Non-Accidents:")
    // Display all Non-Accidents for weekdays
    GetWeekdaysIncidentTypesTotalsWithAvg(spark, "PHL-Weekdays.json").show()
    println("PHL Weekends - Non-Accidents:")
    // Display all Non-Accidents for weekends
    GetWeekendIncidentTypesTotalsWithAvg(spark, "PHL-Weekend.json").show()
    println("PHL Rush-Hour - Non-Accidents:")
    // Display all Non-Accidents during Rush-Hour (7am-10am, 4pm-7pm)
    GetRushHourIncidents(spark, "PHL-Weekdays.json").show()

  }
}
