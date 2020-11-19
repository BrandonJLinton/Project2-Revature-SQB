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

    // Non-Accident: NYC
    println("NYC Full Week - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Entire Time)
    GetIncidentTypesTotal(spark, "NYC-FullWeek.json").show(100, false)
    println("NYC Weekdays - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Weekdays vs Weekends)
    GetWeekdaysIncidentTypesTotalsWithAvg(spark, "NYC-Weekdays.json").show(100, false)
    println("NYC Weekends - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Weekdays vs Weekends)
    GetWeekendIncidentTypesTotalsWithAvg(spark, "NYC-Weekend.json").show(100, false)
    println("NYC Rush-Hour - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Rush-Hour)
    GetRushHourIncidents(spark, "NYC-Weekdays.json").show(100, false)

    // Non-Accident: HOU
    println("HOU Full Week - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Entire Time)
    GetIncidentTypesTotal(spark, "HOU-FullWeek.json").show(100, false)
    println("HOU Weekdays - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Weekdays vs Weekends)
    GetWeekdaysIncidentTypesTotalsWithAvg(spark, "HOU-Weekdays.json").show(100, false)
    println("HOU Weekends - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Weekdays vs Weekends)
    GetWeekendIncidentTypesTotalsWithAvg(spark, "HOU-Weekend.json").show(100, false)
    println("HOU Rush-Hour - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Rush-Hour)
    GetRushHourIncidents(spark, "HOU-Weekdays.json").show(100, false)

    // Non-Accident: PHL
    println("PHL Full Week - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Entire Time)
    GetIncidentTypesTotal(spark, "PHL-FullWeek.json").show(100, false)
    println("PHL Weekdays - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Weekdays vs Weekends)
    GetWeekdaysIncidentTypesTotalsWithAvg(spark, "PHL-Weekdays.json").show(100, false)
    println("PHL Weekends - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Weekdays vs Weekends)
    GetWeekendIncidentTypesTotalsWithAvg(spark, "PHL-Weekend.json").show(100, false)
    println("PHL Rush-Hour - Non-Accidents:")
    // Count of Total mentions for each type of Incident (Timeline: Rush-Hour)
    GetRushHourIncidents(spark, "PHL-Weekdays.json").show(100, false)

  }
}
