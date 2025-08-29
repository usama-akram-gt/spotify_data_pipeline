package com.spotify.pipeline.transforms

import com.spotify.scio._
import com.spotify.scio.jdbc._
import com.spotify.scio.values.SCollection
import com.typesafe.config.ConfigFactory
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.time.{DayOfWeek, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}

/**
 * StreamingHistoryTransform - A Scio pipeline for transforming Spotify streaming history data
 * 
 * This pipeline:
 * 1. Reads streaming history from PostgreSQL
 * 2. Extracts time-based patterns and calculates metrics
 * 3. Computes user listening stats and track popularity
 * 4. Writes results back to analytics tables
 */
object StreamingHistoryTransform {
  // Logger
  private val logger = LoggerFactory.getLogger(this.getClass)
  
  // Load configuration
  private val config = ConfigFactory.load()
  private val dbConfig = config.getConfig("spotify.pipeline.database")
  private val rawDbConfig = dbConfig.getConfig("raw")
  private val analyticsDbConfig = dbConfig.getConfig("analytics")
  private val beamConfig = config.getConfig("spotify.pipeline.beam")
  private val streamingHistoryConfig = config.getConfig("spotify.pipeline.streaming_history")
  
  // Case classes for data models
  case class StreamingEvent(
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
    msPlayed: Option[Int],
    reasonStart: Option[String],
    reasonEnd: Option[String],
    shuffle: Option[Boolean],
    skipped: Option[Boolean],
    platform: Option[String],
    country: Option[String],
    timeOfDay: String,
    isWeekend: Boolean,
    completionRatio: Option[Double]
  )
  
  case class UserListeningStats(
    date: String,
    userId: String,
    totalTracks: Int,
    totalTimeMs: Long,
    avgTrackDurationMs: Double,
    favoriteArtistId: Option[String],
    favoriteGenre: Option[String],
    morningTracks: Int,
    afternoonTracks: Int,
    eveningTracks: Int,
    nightTracks: Int
  )
  
  case class TrackPopularity(
    date: String,
    trackId: String,
    totalStreams: Int,
    uniqueListeners: Int,
    avgCompletionRate: Double,
    skipRate: Double
  )
  
  def main(args: Array[String]): Unit = {
    // Parse date argument
    val date = if (args.length > 0) args(0) else {
      val today = java.time.LocalDate.now()
      today.format(DateTimeFormatter.ISO_LOCAL_DATE)
    }
    
    logger.info(s"Starting StreamingHistoryTransform for date: $date")
    
    // Set up pipeline options
    val options = PipelineOptionsFactory.create()
    options.setJobName(s"spotify-streaming-history-transform-$date")
    
    // Create ScioContext
    val sc = ScioContext(options)
    
    // Read streaming history from PostgreSQL
    val rawJdbcOptions = JdbcConnectionOptions(
      username = rawDbConfig.getString("user"),
      password = rawDbConfig.getString("password"),
      driverClass = rawDbConfig.getString("driver")
    )
    
    val analyticsJdbcOptions = JdbcConnectionOptions(
      username = analyticsDbConfig.getString("user"),
      password = analyticsDbConfig.getString("password"),
      driverClass = analyticsDbConfig.getString("driver")
    )
    
    val streamingEvents: SCollection[StreamingEvent] = sc.jdbcSelect(
      rawDbConfig.getString("url"),
      streamingHistoryConfig.getString("sql_query"),
      statementPreparator = statement => { statement.setString(1, date) },
      connectionOptions = rawJdbcOptions
    )(rs => 
      StreamingEvent(
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
    
    // Enrich streaming events with derived data
    val enrichedEvents: SCollection[EnrichedStreamingEvent] = streamingEvents.map { event =>
      val localDateTime = event.timestamp.toLocalDateTime
      
      // Determine time of day category
      val hour = localDateTime.getHour
      val timeOfDay = hour match {
        case h if h >= 5 && h < 12 => "morning"
        case h if h >= 12 && h < 17 => "afternoon"
        case h if h >= 17 && h < 22 => "evening"
        case _ => "night"
      }
      
      // Determine if weekend
      val isWeekend = {
        val dayOfWeek = localDateTime.getDayOfWeek
        dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY
      }
      
      // Calculate completion ratio (estimated)
      val completionRatio = event.msPlayed.map { ms =>
        math.min(1.0, ms.toDouble / 180000.0) // Assume 3 min average track length
      }
      
      // Format date in ISO format (YYYY-MM-DD)
      val date = localDateTime.toLocalDate.format(DateTimeFormatter.ISO_LOCAL_DATE)
      
      EnrichedStreamingEvent(
        event.eventId,
        event.userId,
        event.trackId,
        event.timestamp,
        date,
        event.msPlayed,
        event.reasonStart,
        event.reasonEnd,
        event.shuffle,
        event.skipped,
        event.platform,
        event.country,
        timeOfDay,
        isWeekend,
        completionRatio
      )
    }
    
    // Calculate user listening stats
    val userListeningStats: SCollection[UserListeningStats] = enrichedEvents
      .keyBy(e => (e.date, e.userId))
      .groupByKey
      .map { case ((date, userId), events) =>
        // Initialize counters
        var totalTracks = 0
        var totalTimeMs = 0L
        val trackDurations = scala.collection.mutable.ArrayBuffer.empty[Int]
        val artists = scala.collection.mutable.Map.empty[String, Int]
        val genres = scala.collection.mutable.Map.empty[String, Int]
        val timeOfDayCounts = scala.collection.mutable.Map(
          "morning" -> 0,
          "afternoon" -> 0,
          "evening" -> 0,
          "night" -> 0
        )
        
        // Process events
        events.foreach { event =>
          totalTracks += 1
          
          // Accumulate play time
          event.msPlayed.foreach { ms =>
            totalTimeMs += ms
            trackDurations += ms
          }
          
          // Count by time of day
          timeOfDayCounts(event.timeOfDay) += 1
          
          // TODO: In a real implementation, we would join with artist data
          // For now, we'll leave these as None
        }
        
        // Calculate averages
        val avgTrackDurationMs = if (trackDurations.nonEmpty) {
          trackDurations.sum.toDouble / trackDurations.size
        } else {
          0.0
        }
        
        // Determine favorites (would be derived from actual data)
        val favoriteArtistId = None // In reality, would be: artists.maxBy(_._2)._1
        val favoriteGenre = None // In reality, would be: genres.maxBy(_._2)._1
        
        UserListeningStats(
          date,
          userId,
          totalTracks,
          totalTimeMs,
          avgTrackDurationMs,
          favoriteArtistId,
          favoriteGenre,
          timeOfDayCounts("morning"),
          timeOfDayCounts("afternoon"),
          timeOfDayCounts("evening"),
          timeOfDayCounts("night")
        )
      }
    
    // Calculate track popularity
    val trackPopularity: SCollection[TrackPopularity] = enrichedEvents
      .keyBy(e => (e.date, e.trackId))
      .groupByKey
      .map { case ((date, trackId), events) =>
        // Initialize counters
        var totalStreams = 0
        val uniqueListeners = scala.collection.mutable.Set.empty[String]
        val completionRates = scala.collection.mutable.ArrayBuffer.empty[Double]
        var skips = 0
        
        // Process events
        events.foreach { event =>
          totalStreams += 1
          uniqueListeners += event.userId
          
          // Track completion rates
          event.completionRatio.foreach(completionRates += _)
          
          // Track skips
          if (event.skipped.getOrElse(false)) {
            skips += 1
          }
        }
        
        // Calculate statistics
        val avgCompletionRate = if (completionRates.nonEmpty) {
          completionRates.sum / completionRates.size
        } else {
          0.0
        }
        
        val skipRate = if (totalStreams > 0) {
          skips.toDouble / totalStreams
        } else {
          0.0
        }
        
        TrackPopularity(
          date,
          trackId,
          totalStreams,
          uniqueListeners.size,
          avgCompletionRate,
          skipRate
        )
      }
    
    // Write user listening stats to analytics database
    userListeningStats.saveAsJdbc(
      analyticsDbConfig.getString("url"),
      "analytics.daily_listening_stats",
      connectionOptions = analyticsJdbcOptions
    )
    
    // Write track popularity to analytics database
    trackPopularity.saveAsJdbc(
      analyticsDbConfig.getString("url"),
      "analytics.track_popularity",
      connectionOptions = analyticsJdbcOptions
    )
    
    // Execute the pipeline
    val result = sc.run().waitUntilFinish()
    logger.info(s"Pipeline completed with state: ${result.toString}")
  }
} 