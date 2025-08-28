package com.spotify.pipeline.transforms

import com.spotify.scio._
import com.spotify.scio.jdbc._
import com.spotify.scio.parquet._
import com.spotify.scio.values.SCollection
import com.typesafe.config.ConfigFactory
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, format}
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}

/**
 * Enhanced StreamingHistoryTransform for Azure Data Lake integration
 * 
 * This pipeline:
 * 1. Reads streaming history from PostgreSQL
 * 2. Enriches data with time-based patterns
 * 3. Writes to Azure Data Lake Storage (Parquet)
 * 4. Loads aggregated data into Databricks for analytics
 */
object AzureStreamingHistoryTransform {
  private val logger = LoggerFactory.getLogger(this.getClass)
  
  // Load Azure-specific configuration
  private val config = ConfigFactory.load("application-azure")
  private val pipelineConfig = config.getConfig("spotify.pipeline")
  private val azureConfig = pipelineConfig.getConfig("azure.data_lake")
  private val dbConfig = pipelineConfig.getConfig("database")
  
  // Case classes for Azure integration
  case class RawStreamingEvent(
    eventId: String,
    userId: String,
    trackId: String,
    timestamp: Timestamp,
    msPlayed: Option[Int],
    reasonStart: Option[String],
    reasonEnd: Option[String],
    shuffle: Option[Boolean],
    skipped: Option[Boolean],
    platform: Option[String],
    country: Option[String]
  )

  case class EnrichedStreamingEvent(
    eventId: String,
    userId: String,
    trackId: String,
    timestamp: Timestamp,
    date: String,
    year: Int,
    month: Int,
    day: Int,
    hour: Int,
    msPlayed: Option[Int],
    reasonStart: Option[String],
    reasonEnd: Option[String],
    shuffle: Option[Boolean],
    skipped: Option[Boolean],
    platform: Option[String],
    country: Option[String],
    timeOfDay: String,
    isWeekend: Boolean,
    completionRatio: Option[Double],
    processedAt: Timestamp
  )
  
  case class UserDailyStats(
    date: String,
    userId: String,
    country: Option[String],
    totalTracks: Int,
    totalTimeMs: Long,
    avgTrackDurationMs: Double,
    uniqueTracks: Int,
    morningTracks: Int,
    afternoonTracks: Int,
    eveningTracks: Int,
    nightTracks: Int,
    skippedTracks: Int,
    completedTracks: Int,
    avgCompletionRate: Double
  )
  
  case class TrackDailyStats(
    date: String,
    trackId: String,
    totalStreams: Int,
    uniqueListeners: Int,
    totalListeningTimeMs: Long,
    avgCompletionRate: Double,
    skipRate: Double,
    topCountries: String, // JSON string of top countries
    peakHour: Int
  )

  def main(args: Array[String]): Unit = {
    // Parse arguments
    val date = if (args.nonEmpty) args(0) else {
      LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
    }
    
    logger.info(s"Starting Azure Streaming History Transform for date: $date")
    
    // Setup pipeline options
    val options = PipelineOptionsFactory.create()
    options.setJobName(s"azure-streaming-transform-$date")
    
    // Configure Hadoop for Azure authentication
    System.setProperty("hadoop.home.dir", "/tmp/hadoop")
    
    val sc = ScioContext(options)
    
    // Setup JDBC connection options
    val rawJdbcOptions = JdbcConnectionOptions(
      username = dbConfig.getString("raw.user"),
      password = dbConfig.getString("raw.password"),
      driverClass = dbConfig.getString("raw.driver")
    )
    
    val analyticsJdbcOptions = JdbcConnectionOptions(
      username = dbConfig.getString("analytics.user"),
      password = dbConfig.getString("analytics.password"),
      driverClass = dbConfig.getString("analytics.driver")
    )
    
    // Read raw streaming events from PostgreSQL
    val rawEvents: SCollection[RawStreamingEvent] = sc.jdbcSelect(
      dbConfig.getString("raw.url"),
      pipelineConfig.getString("streaming_history.sql_query"),
      statementPreparator = statement => { statement.setString(1, date) },
      connectionOptions = rawJdbcOptions
    )(rs => 
      RawStreamingEvent(
        rs.getString("event_id"),
        rs.getString("user_id"),
        rs.getString("track_id"),
        rs.getTimestamp("timestamp"),
        Option(rs.getObject("ms_played")).map(_.asInstanceOf[Integer].toInt),
        Option(rs.getString("reason_start")),
        Option(rs.getString("reason_end")),
        Option(rs.getObject("shuffle")).map(_.asInstanceOf[Boolean]),
        Option(rs.getObject("skipped")).map(_.asInstanceOf[Boolean]),
        Option(rs.getString("platform")),
        Option(rs.getString("country"))
      )
    )
    
    // Enrich events with time-based features
    val enrichedEvents: SCollection[EnrichedStreamingEvent] = rawEvents.map { event =>
      val localDateTime = event.timestamp.toLocalDateTime
      val zonedDateTime = localDateTime.atZone(ZoneId.of("UTC"))
      
      // Extract time components
      val year = localDateTime.getYear
      val month = localDateTime.getMonthValue
      val day = localDateTime.getDayOfMonth
      val hour = localDateTime.getHour
      
      // Categorize time of day
      val timeOfDay = hour match {
        case h if h >= 5 && h < 12 => "morning"
        case h if h >= 12 && h < 17 => "afternoon" 
        case h if h >= 17 && h < 22 => "evening"
        case _ => "night"
      }
      
      // Weekend detection
      val isWeekend = {
        val dayOfWeek = localDateTime.getDayOfWeek.getValue
        dayOfWeek == 6 || dayOfWeek == 7 // Saturday or Sunday
      }
      
      // Calculate completion ratio (assuming average track length of 3 minutes)
      val completionRatio = event.msPlayed.map { ms =>
        math.min(1.0, ms.toDouble / 180000.0)
      }
      
      EnrichedStreamingEvent(
        event.eventId,
        event.userId,
        event.trackId,
        event.timestamp,
        date,
        year,
        month,
        day,
        hour,
        event.msPlayed,
        event.reasonStart,
        event.reasonEnd,
        event.shuffle,
        event.skipped,
        event.platform,
        event.country,
        timeOfDay,
        isWeekend,
        completionRatio,
        new Timestamp(System.currentTimeMillis())
      )
    }
    
    // Write enriched events to Azure Data Lake (Parquet)
    val adlsPath = azureConfig.getString("base_url") + azureConfig.getString("paths.silver") + "/streaming_events"
    val partitionedPath = s"$adlsPath/year=$year/month=${"%02d".format(month)}/day=${"%02d".format(day)}"
    
    logger.info(s"Writing enriched events to: $partitionedPath")
    
    enrichedEvents.saveAsTypedParquet(partitionedPath)
    
    // Calculate user daily statistics
    val userStats: SCollection[UserDailyStats] = enrichedEvents
      .keyBy(e => (e.date, e.userId))
      .groupByKey
      .map { case ((date, userId), events) =>
        val eventList = events.toList
        val totalTracks = eventList.size
        val totalTimeMs = eventList.flatMap(_.msPlayed).sum
        val uniqueTracks = eventList.map(_.trackId).toSet.size
        val country = eventList.headOption.flatMap(_.country)
        
        // Time of day breakdown
        val timeOfDayCounts = eventList.groupBy(_.timeOfDay).view.mapValues(_.size).toMap
        val morningTracks = timeOfDayCounts.getOrElse("morning", 0)
        val afternoonTracks = timeOfDayCounts.getOrElse("afternoon", 0)
        val eveningTracks = timeOfDayCounts.getOrElse("evening", 0)
        val nightTracks = timeOfDayCounts.getOrElse("night", 0)
        
        // Skip and completion analysis
        val skippedTracks = eventList.count(_.skipped.getOrElse(false))
        val completedTracks = totalTracks - skippedTracks
        val completionRates = eventList.flatMap(_.completionRatio)
        val avgCompletionRate = if (completionRates.nonEmpty) {
          completionRates.sum / completionRates.size
        } else 0.0
        
        val avgTrackDuration = if (totalTracks > 0) totalTimeMs.toDouble / totalTracks else 0.0
        
        UserDailyStats(
          date,
          userId,
          country,
          totalTracks,
          totalTimeMs,
          avgTrackDuration,
          uniqueTracks,
          morningTracks,
          afternoonTracks,
          eveningTracks,
          nightTracks,
          skippedTracks,
          completedTracks,
          avgCompletionRate
        )
      }
    
    // Calculate track daily statistics
    val trackStats: SCollection[TrackDailyStats] = enrichedEvents
      .keyBy(e => (e.date, e.trackId))
      .groupByKey
      .map { case ((date, trackId), events) =>
        val eventList = events.toList
        val totalStreams = eventList.size
        val uniqueListeners = eventList.map(_.userId).toSet.size
        val totalListeningTime = eventList.flatMap(_.msPlayed).sum
        
        // Completion and skip rates
        val completionRates = eventList.flatMap(_.completionRatio)
        val avgCompletionRate = if (completionRates.nonEmpty) {
          completionRates.sum / completionRates.size
        } else 0.0
        
        val skippedCount = eventList.count(_.skipped.getOrElse(false))
        val skipRate = if (totalStreams > 0) skippedCount.toDouble / totalStreams else 0.0
        
        // Geographic analysis
        val countryCounts = eventList.flatMap(_.country).groupBy(identity).view.mapValues(_.size)
        val topCountries = countryCounts.toSeq.sortBy(-_._2).take(3)
          .map { case (country, count) => s"$country:$count" }.mkString(",")
        
        // Peak hour analysis
        val hourCounts = eventList.groupBy(_.hour).view.mapValues(_.size)
        val peakHour = if (hourCounts.nonEmpty) hourCounts.maxBy(_._2)._1 else 0
        
        TrackDailyStats(
          date,
          trackId,
          totalStreams,
          uniqueListeners,
          totalListeningTime,
          avgCompletionRate,
          skipRate,
          topCountries,
          peakHour
        )
      }
    
    // Write user stats to Databricks
    userStats.saveAsJdbc(
      dbConfig.getString("analytics.url"),
      s"${pipelineConfig.getString("streaming_history.output.databricks.schema")}.${pipelineConfig.getString("streaming_history.output.databricks.table_user_stats")}",
      connectionOptions = analyticsJdbcOptions
    )
    
    // Write track stats to Databricks  
    trackStats.saveAsJdbc(
      dbConfig.getString("analytics.url"),
      s"${pipelineConfig.getString("streaming_history.output.databricks.schema")}.${pipelineConfig.getString("streaming_history.output.databricks.table_track_popularity")}",
      connectionOptions = analyticsJdbcOptions
    )
    
    // Execute pipeline
    val result = sc.run().waitUntilFinish()
    logger.info(s"Pipeline completed with state: ${result.getState}")
    
    // Log metrics
    val metrics = result.getMetrics
    logger.info(s"Pipeline metrics: ${metrics}")
  }
}