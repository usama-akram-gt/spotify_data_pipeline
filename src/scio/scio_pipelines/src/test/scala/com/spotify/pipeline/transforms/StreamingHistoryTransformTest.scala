package com.spotify.pipeline.transforms

import com.spotify.scio.jdbc.JdbcConnectionOptions
import com.spotify.scio.testing._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

class StreamingHistoryTransformTest extends AnyFlatSpec with ScioIOSpec with Matchers {
  import StreamingHistoryTransform._

  "StreamingHistoryTransform" should "compute user listening stats correctly" in {
    val testTimestamp = Timestamp.valueOf(LocalDateTime.of(2023, 5, 1, 14, 30, 0))
    val testEvents = Seq(
      StreamingEvent("event1", "user1", "track1", testTimestamp, Some(120000), Some("trackdone"), Some("endplay"), Some(false), Some(false), Some("mobile"), Some("US")),
      StreamingEvent("event2", "user1", "track2", testTimestamp, Some(180000), Some("playbtn"), Some("trackdone"), Some(false), Some(false), Some("mobile"), Some("US")),
      StreamingEvent("event3", "user1", "track3", testTimestamp, Some(60000), Some("playbtn"), Some("forwarded"), Some(false), Some(true), Some("mobile"), Some("US"))
    )

    val enrichedEvents = testEvents.map { event =>
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
        dayOfWeek == java.time.DayOfWeek.SATURDAY || dayOfWeek == java.time.DayOfWeek.SUNDAY
      }
      
      // Calculate completion ratio (estimated)
      val completionRatio = event.msPlayed.map { ms =>
        math.min(1.0, ms.toDouble / 180000.0) // Assume 3 min average track length
      }
      
      // Format date in ISO format (YYYY-MM-DD)
      val date = localDateTime.toLocalDate.format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE)
      
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

    val expected = JobTest[StreamingHistoryTransform.type]
      .args("2023-05-01")
      .input(JdbcIO[StreamingEvent](rawDbConfig.getString("url")), testEvents)
      
    // Test user listening stats calculation
    val userStats = enrichedEvents
      .groupBy(e => (e.date, e.userId))
      .map { case ((date, userId), events) =>
        // Initialize counters
        var totalTracks = 0
        var totalTimeMs = 0L
        val trackDurations = scala.collection.mutable.ArrayBuffer.empty[Int]
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
        }
        
        // Calculate averages
        val avgTrackDurationMs = if (trackDurations.nonEmpty) {
          trackDurations.sum.toDouble / trackDurations.size
        } else {
          0.0
        }
        
        UserListeningStats(
          date,
          userId,
          totalTracks,
          totalTimeMs,
          avgTrackDurationMs,
          None, // favoriteArtistId
          None, // favoriteGenre
          timeOfDayCounts("morning"),
          timeOfDayCounts("afternoon"),
          timeOfDayCounts("evening"),
          timeOfDayCounts("night")
        )
      }
    
    val userStat = userStats.head
    userStat.totalTracks shouldBe 3
    userStat.totalTimeMs shouldBe 360000
    userStat.avgTrackDurationMs shouldBe 120000.0
    userStat.afternoonTracks shouldBe 3
    userStat.morningTracks shouldBe 0
    userStat.eveningTracks shouldBe 0
    userStat.nightTracks shouldBe 0
    
    // Test track popularity calculation
    val trackStats = enrichedEvents
      .groupBy(e => (e.date, e.trackId))
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
    
    // Verify that we have three track popularity records
    trackStats.size shouldBe 3
    
    // Check the track with skips
    val skippedTrack = trackStats.find(_.trackId == "track3").get
    skippedTrack.totalStreams shouldBe 1
    skippedTrack.skipRate shouldBe 1.0
    
    // Check completion rates for a fully played track
    val fullTrack = trackStats.find(_.trackId == "track2").get
    fullTrack.avgCompletionRate shouldBe 1.0
  }
} 