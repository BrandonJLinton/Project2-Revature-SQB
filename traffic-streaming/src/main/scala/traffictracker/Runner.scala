/*
Quan Vu, Project2 Traffic Incidents Tracking
This program is designed for live traffic streaming only.
Will perform queries such as finding the total accident counts for each city (NYC, HOUSTON, PHILADELPHIA),
finding the general location for each of the accident in each city, finding the type of non accident that occurred,
and finding the time it takes for an accident to be cleared.
 */

package traffictracker

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.nio.file.{Files, Paths}

import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, explode, regexp_extract}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Runner {
  def main(args: Array[String]): Unit = {

      //We have some API keys, secrets, tokens from the Twitter API
      //We definitely do not want to hardcode these.
      //If you *must* hardcode these, then gitignore the files that contain them
      //One nice way to handle strings we want to keep secret, like API keys
      //is to pass them into your program as environment variables

      //We get access environment variables using System.getenv
      //We can set environment variables in our run config in IntelliJ

      val bearerToken = System.getenv("BEARER_TOKEN")
      println(s"Bearer token is : $bearerToken")

      //Run tweetStreamToDir in the background:
      Future {
        tweetStreamToDir(bearerToken)
      }

      val spark = SparkSession.builder()
        .appName("Traffic Spark Structured Streaming")
        .master("local[4]") // when streaming some threads need to be Receivers that listen, so we need more than typical
        .getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("WARN")

      //When streaming, we can't infer the schema so let's create a static dataframe
      // and use its inferred schema.  Requires some files in twitterstream
      val staticDf = spark.read.json("twitterstream")

/*
      // My UserID: 1325862575762935811, UserName: QuanVu72601925
      val streamDf4 = spark.readStream.schema(staticDf.schema).json("twitterstream")
      //.withColumn("data", explode($"data"))
      streamDf4.filter(streamDf4("data.author_id") === "1325862575762935811").createOrReplaceTempView("view1")
*/

      // Filter out the data for HOUSTON
      val streamDf1 = spark.readStream.schema(staticDf.schema).json("twitterstream")
        streamDf1.filter(streamDf1("data.author_id") === "249818911").createOrReplaceTempView("view1")

      // Filter out the data for PHILADELPHIA
      val streamDf2 = spark.readStream.schema(staticDf.schema).json("twitterstream")
      streamDf2.filter(streamDf2("data.author_id") === "249854777").createOrReplaceTempView("view2")

      // Filter out the data for NYC
      val streamDf3 = spark.readStream.schema(staticDf.schema).json("twitterstream")
      //.withColumn("data", explode($"data"))
      streamDf3.filter(streamDf3("data.author_id") === "42640432").createOrReplaceTempView("view3")

      // Querying on the streaming data view (data keeps coming in and query keep executing on them)
    //------------------------------------------------------------------------------------------------------------------
      // Find the number of accidents occurred in the 9 hours timeframe
      val accident1 = spark.sql("SELECT COUNT(*) AS accident_count_HOUSTON " +
        "FROM view1 " +
        "WHERE view1.data.text LIKE '%accident%' OR view1.data.text LIKE 'Accident%' " +
        "AND view1.data.text NOT LIKE 'Accident cleared%' " +
        "OR view1.data.text LIKE 'Overturned%'")
        .writeStream.outputMode("complete").format("console").start()
      //accident1.awaitTermination()   // This ends the app which ends the Future

      val accident2 = spark.sql("SELECT COUNT(*) AS accident_count_PHILADELPHIA " +
        "FROM view2 " +
        "WHERE view2.data.text LIKE '%accident%' OR view2.data.text LIKE 'Accident%' " +
        "AND view2.data.text NOT LIKE 'Accident cleared%' " +
        "OR view2.data.text LIKE 'Overturned%'")
        .writeStream.outputMode("complete").format("console").start()

      val accident3 = spark.sql("SELECT COUNT(*) AS accident_count_NYC " +
        "FROM view3 " +
        "WHERE view3.data.text LIKE '%accident%' OR view3.data.text LIKE 'Accident%' " +
        "AND view3.data.text NOT LIKE 'Accident cleared%' " +
        "OR view3.data.text LIKE 'Overturned%'")
        .writeStream.outputMode("complete").format("console").start()
  //--------------------------------------------------------------------------------------------------------------------

      // Find the counts for all non-accident types in 9 hours streaming
      val incident1 = spark.sql("SELECT REGEXP_EXTRACT(view1.data.text, '^([^,]+)') AS incidents_HOUSTON, COUNT(view1.data.id) AS occurrences " +
        "FROM view1 " +
        "WHERE REPLACE(REPLACE(REPLACE(view1.data.text, ', right lane blocked', ' '), ', left lane blocked', ' '), ', center lane blocked', ' ') LIKE 'Disabled%' " +
        "OR view1.data.text LIKE '%construction%' " +
        "OR REPLACE(view1.data.text, 'blocked', 'closed') LIKE 'Off-ramp%' " +
        "OR REPLACE(view1.data.text, 'blocked', 'closed') LIKE 'On-ramp%' " +
        "OR view1.data.text LIKE 'Bridge%' " +
        "OR view1.data.text LIKE 'Accident cleared%' " +
        "AND view1.data.text NOT LIKE '%accident%' AND view1.data.text NOT LIKE 'Accident%' " +
        "AND view1.data.text NOT LIKE 'Overturned%' " +
        "AND view1.data.text NOT LIKE 'See traffic problems%' " +
        "GROUP BY incidents_HOUSTON " +
        "ORDER BY occurrences DESC")
        .writeStream.outputMode("complete").format("console").start()

    val incident2 = spark.sql("SELECT REGEXP_EXTRACT(view2.data.text, '^([^,]+)') AS incidents_PHILADELPHIA, COUNT(view2.data.id) AS occurrences " +
      "FROM view2 " +
      "WHERE REPLACE(REPLACE(REPLACE(view2.data.text, ', right lane blocked', ' '), ', left lane blocked', ' '), ', center lane blocked', ' ') LIKE 'Disabled%' " +
      "OR view2.data.text LIKE '%construction%' " +
      "OR REPLACE(view2.data.text, 'blocked', 'closed') LIKE 'Off-ramp%' " +
      "OR REPLACE(view2.data.text, 'blocked', 'closed') LIKE 'On-ramp%' " +
      "OR view2.data.text LIKE 'Bridge%' " +
      "OR view2.data.text LIKE 'Accident cleared%' " +
      "AND view2.data.text NOT LIKE '%accident%' AND view2.data.text NOT LIKE 'Accident%' " +
      "AND view2.data.text NOT LIKE 'Overturned%' " +
      "AND view2.data.text NOT LIKE 'See traffic problems%' " +
      "GROUP BY incidents_PHILADELPHIA " +
      "ORDER BY occurrences DESC")
      .writeStream.outputMode("complete").format("console").start()

    val incident3 = spark.sql("SELECT REGEXP_EXTRACT(view3.data.text, '^([^,]+)') AS incidents_NYC, COUNT(view3.data.id) AS occurrences " +
      "FROM view3 " +
      "WHERE REPLACE(REPLACE(REPLACE(view3.data.text, ', right lane blocked', ' '), ', left lane blocked', ' '), ', center lane blocked', ' ') LIKE 'Disabled%' " +
      "OR view3.data.text LIKE '%construction%' " +
      "OR REPLACE(view3.data.text, 'blocked', 'closed') LIKE 'Off-ramp%' " +
      "OR REPLACE(view3.data.text, 'blocked', 'closed') LIKE 'On-ramp%' " +
      "OR view3.data.text LIKE 'Bridge%' " +
      "OR view3.data.text LIKE 'Accident cleared%' " +
      "AND view3.data.text NOT LIKE '%accident%' AND view3.data.text NOT LIKE 'Accident%' " +
      "AND view3.data.text NOT LIKE 'Overturned%' " +
      "AND view3.data.text NOT LIKE 'See traffic problems%' " +
      "GROUP BY incidents_NYC " +
      "ORDER BY occurrences DESC")
      .writeStream.outputMode("complete").format("console").start()
    //incident3.awaitTermination()
  //--------------------------------------------------------------------------------------------------------------------

      // Finding the accident counts per general location for each city (HOUSTON, PHILADELPHIA, NYC)
    val location1 = spark.sql("SELECT view1.data.entities.hashtags.tag[0] AS general_location_HOUSTON, COUNT(view1.data.id) AS accident_counts " +
      "FROM view1 " +
      "WHERE view1.data.text LIKE '%accident%' OR view1.data.text LIKE 'Accident%' " +
      "AND view1.data.text NOT LIKE 'Accident cleared%' " +
      "OR view1.data.text LIKE 'Overturned%' " +
      "GROUP BY general_location_HOUSTON " +
      "ORDER BY accident_counts DESC")
      .writeStream.outputMode("complete").format("console").start()

    val location2 = spark.sql("SELECT view2.data.entities.hashtags.tag[0] AS general_location_PHILADELPHIA, COUNT(view2.data.id) AS accident_counts " +
      "FROM view2 " +
      "WHERE view2.data.text LIKE '%accident%' OR view2.data.text LIKE 'Accident%' " +
      "AND view2.data.text NOT LIKE 'Accident cleared%' " +
      "OR view2.data.text LIKE 'Overturned%' " +
      "GROUP BY general_location_PHILADELPHIA " +
      "ORDER BY accident_counts DESC")
      .writeStream.outputMode("complete").format("console").start()

    val location3 = spark.sql("SELECT view3.data.entities.hashtags.tag[0] AS general_location_NYC, COUNT(view3.data.id) AS accident_counts " +
      "FROM view3 " +
      "WHERE view3.data.text LIKE '%accident%' OR view3.data.text LIKE 'Accident%' " +
      "AND view3.data.text NOT LIKE 'Accident cleared%' " +
      "OR view3.data.text LIKE 'Overturned%' " +
      "GROUP BY general_location_NYC " +
      "ORDER BY accident_counts DESC")
      .writeStream.outputMode("complete").format("console").start()

    //location3.awaitTermination(3600000) // 9-hour: 32400000 millisecs
    //------------------------------------------------------------------------------------------------------------------

    // Nested query: Find occurrences of cleared accidents -- get table with location tag, regex extract for the streets,
    // created_at time. Use WHERE to find accident cleared key.
    // Outer query: Find the accident tweets (no cleared ones), compare the location tag and regex extracted street.
    // Need table with columns: location tag, regex street, time of accident, time when accident cleared.
    // Regex to use: (?<=on)(.*)(?=at) get the street after the first 'on' and before first 'at'
    // Use REPLACE on 'between' and 'before' to change to 'at' for simplicity

    // Compare time when accident occurred and when accident was cleared
    val compare1 = spark.sql("SELECT view1.data.entities.hashtags.tag[0] AS general_location_HOUSTON, table1.road AS road, " +
      "view1.data.created_at AS accident_time, table1.cleared_time AS cleared_time " +
      "FROM view1, (SELECT view1.data.entities.hashtags.tag[0] AS general_location, " +
      "REGEXP_EXTRACT(REPLACE(REPLACE(view1.data.text, ' between ', ' at '), ' before ', ' at '), '(?<=on)(.*)(?=at)') AS road, " +
      "view1.data.created_at AS cleared_time " +
      "FROM view1 " +
      "WHERE view1.data.text LIKE 'Accident cleared%') AS table1 " +
      "WHERE view1.data.entities.hashtags.tag[0] = table1.general_location " +
      "AND view1.data.text LIKE CONCAT('%', table1.road, '%') " +
      "AND view1.data.text NOT LIKE 'Accident cleared%'")
      .writeStream.outputMode("append").format("console").start()

    val compare2 = spark.sql("SELECT view2.data.entities.hashtags.tag[0] AS general_location_PHILADELPHIA, table1.road AS road, " +
      "view2.data.created_at AS accident_time, table1.cleared_time AS cleared_time " +
      "FROM view2, (SELECT view2.data.entities.hashtags.tag[0] AS general_location, " +
      "REGEXP_EXTRACT(REPLACE(REPLACE(view2.data.text, ' between ', ' at '), ' before ', ' at '), '(?<=on)(.*)(?=at)') AS road, " +
      "view2.data.created_at AS cleared_time " +
      "FROM view2 " +
      "WHERE view2.data.text LIKE 'Accident cleared%') AS table1 " +
      "WHERE view2.data.entities.hashtags.tag[0] = table1.general_location " +
      "AND view2.data.text LIKE CONCAT('%', table1.road, '%') " +
      "AND view2.data.text NOT LIKE 'Accident cleared%'")
      .writeStream.outputMode("append").format("console").start()

    val compare3 = spark.sql("SELECT view3.data.entities.hashtags.tag[0] AS general_location_NYC, table1.road AS road, " +
      "view3.data.created_at AS accident_time, table1.cleared_time AS cleared_time " +
      "FROM view3, (SELECT view3.data.entities.hashtags.tag[0] AS general_location, " +
      "REGEXP_EXTRACT(REPLACE(REPLACE(view3.data.text, ' between ', ' at '), ' before ', ' at '), '(?<=on)(.*)(?=at)') AS road, " +
      "view3.data.created_at AS cleared_time " +
      "FROM view3 " +
      "WHERE view3.data.text LIKE 'Accident cleared%') AS table1 " +
      "WHERE view3.data.entities.hashtags.tag[0] = table1.general_location " +
      "AND view3.data.text LIKE CONCAT('%', table1.road, '%') " +
      "AND view3.data.text NOT LIKE 'Accident cleared%'")
      .writeStream.outputMode("append").format("console").start()
    compare3.awaitTermination(34200000) // 9.5 hours

  }

    // Structured streaming using filtered stream to connect to Twitter API and storing streaming data into batches
    // under a directory.
    def tweetStreamToDir(bearerToken: String, dirname: String="twitterstream", linesPerFile: Int=30) = {
        val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
        val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at,entities&expansions=author_id&user.fields=created_at")
        val httpGet = new HttpGet(uriBuilder.build)
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))

        val response = httpClient.execute(httpGet)
        val entity = response.getEntity
        if (null != entity) {
            val reader = new BufferedReader(new InputStreamReader(entity.getContent))
            var line = reader.readLine

            //initial filewriter, will be replaced with new filewriter every linesperfile
            var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
            var lineNumber = 1 //track line number to know when to move to new file
            val millis = System.currentTimeMillis() //identify this job with millis
            while ( {
                line != null
            }) {
                if(lineNumber % linesPerFile == 0) {
                    fileWriter.close()
                    Files.move(
                        Paths.get("tweetstream.tmp"),
                        Paths.get(s"${dirname}/tweetstream-${millis}-${lineNumber/linesPerFile}"))
                    fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
                }
                fileWriter.println(line)
                System.out.println(line)
                line = reader.readLine()
                lineNumber += 1
            }
        }
    }
}
