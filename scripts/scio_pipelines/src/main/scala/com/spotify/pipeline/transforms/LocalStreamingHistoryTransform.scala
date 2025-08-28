package com.spotify.pipeline.transforms

import com.spotify.scio._
import com.spotify.scio.jdbc._
import com.spotify.scio.parquet._
import com.spotify.scio.values.SCollection
import com.typesafe.config.ConfigFactory
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.time.{LocalDateTime, format}
import java.time.format.DateTimeFormatter
import java.io.File
import scala.util.{Failure, Success, Try}

/**
 * Local StreamingHistoryTransform for development on your Mac
 * 
 * This pipeline:
 * 1. Reads streaming history from local PostgreSQL
 * 2. Enriches data with time-based patterns  
 * 3. Writes to local file system (Parquet)
 * 4. Loads aggregated data back to PostgreSQL analytics tables
 * 
 * Usage: sbt "runMain com.spotify.pipeline.transforms.LocalStreamingHistoryTransform 2024-01-15"
 */
object LocalStreamingHistoryTransform {
  private val logger = LoggerFactory.getLogger(this.getClass)
  
  // Load local configuration
  private val config = ConfigFactory.load("application-local")
  private val pipelineConfig = config.getConfig("spotify.pipeline")
  private val localConfig = pipelineConfig.getConfig("local.storage")
  private val dbConfig = pipelineConfig.getConfig("database")
  
  // Case classes (same as Azure version but with local file paths)
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
    topCountries: String,
    peakHour: Int
  )

  def main(args: Array[String]): Unit = {
    // Parse arguments
    val date = if (args.nonEmpty) args(0) else {
      LocalDateTime.now().minusDays(1).format(DateTimeFormatter.ISO_LOCAL_DATE)
    }
    
    logger.info(s"Starting Local Streaming History Transform for date: $date")
    logger.info(s"Output path: ${localConfig.getString("base_path")}")
    
    // Create output directories if they don't exist
    createDirectories()
    
    // Setup pipeline options for local execution
    val options = PipelineOptionsFactory.create()
    options.setJobName(s"local-streaming-transform-$date")
    
    val sc = ScioContext(options)
    
    // Setup JDBC connection options
    val jdbcOptions = JdbcConnectionOptions(
      username = dbConfig.getString("raw.user"),
      password = dbConfig.getString("raw.password"),
      driverClass = dbConfig.getString("raw.driver")
    )
    
    // Read raw streaming events from local PostgreSQL
    val rawEvents: SCollection[RawStreamingEvent] = sc.jdbcSelect(
      dbConfig.getString("raw.url"),
      pipelineConfig.getString("streaming_history.sql_query"),
      statementPreparator = statement => { statement.setString(1, date) },
      connectionOptions = jdbcOptions
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
    
    // Log count of raw events
    val eventCount = rawEvents.count
    eventCount.map { count =>
      logger.info(s"Processing $count streaming events for date $date")
      count
    }
    
    // Enrich events with time-based features
    val enrichedEvents: SCollection[EnrichedStreamingEvent] = rawEvents.map { event =>
      val localDateTime = event.timestamp.toLocalDateTime
      
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
    
    // Write enriched events to local file system (Parquet)
    val outputPath = s"${localConfig.getString("base_path")}/streaming_events/date=$date"
    logger.info(s"Writing enriched events to: $outputPath")
    
    enrichedEvents.saveAsTypedParquet(outputPath)
    
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
    
    // Write user stats back to PostgreSQL
    userStats.saveAsJdbc(
      dbConfig.getString("analytics.url"),
      s"${pipelineConfig.getString("streaming_history.output.analytics.schema")}.${pipelineConfig.getString("streaming_history.output.analytics.table_user_stats")}",
      connectionOptions = jdbcOptions
    )
    
    // Write track stats back to PostgreSQL  
    trackStats.saveAsJdbc(
      dbConfig.getString("analytics.url"),
      s"${pipelineConfig.getString("streaming_history.output.analytics.schema")}.${pipelineConfig.getString("streaming_history.output.analytics.table_track_popularity")}",
      connectionOptions = jdbcOptions
    )
    
    // Execute pipeline
    logger.info("Starting pipeline execution...")
    val result = sc.run().waitUntilFinish()
    logger.info(s"Pipeline completed with state: ${result.getState}")
    
    // Log completion
    logger.info(s"Local pipeline completed successfully!")
    logger.info(s"Check output files in: ${localConfig.getString("base_path")}")
    logger.info(s"Check analytics tables in PostgreSQL schema: analytics")
  }
  
  private def createDirectories(): Unit = {
    val basePath = localConfig.getString("base_path")
    val dirs = Seq(
      basePath,
      s"$basePath/streaming_events",
      localConfig.getString("raw_path"),
      localConfig.getString("processed_path"),
      localConfig.getString("analytics_path")
    )
    
    dirs.foreach { path =>
      val dir = new File(path)
      if (!dir.exists()) {
        dir.mkdirs()
        logger.info(s"Created directory: $path")
      }
    }
  }
}