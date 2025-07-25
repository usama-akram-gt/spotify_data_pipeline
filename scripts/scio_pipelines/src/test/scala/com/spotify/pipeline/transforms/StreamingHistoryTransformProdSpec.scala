package com.spotify.pipeline.transforms

import org.scalatest.Tag
import com.spotify.scio.testing.JobTest
import com.spotify.scio.jdbc.JdbcIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

/**
 * Production tests for StreamingHistoryTransform - more comprehensive, slower
 */
class StreamingHistoryTransformProdSpec extends StreamingHistoryTransformSpecBase {
  
  // Tag for production tests
  object Production extends Tag("Production")
  
  // Test data size for production tests - much larger to test scalability
  val PROD_TEST_SIZE = 5000
  
  // PostgreSQL connection mocks
  val mockRawDbConnUrl = "jdbc:postgresql://localhost:5432/spotify_raw"
  val mockAnalyticsDbConnUrl = "jdbc:postgresql://localhost:5432/spotify_analytics"
  val mockUser = "postgres"
  val mockPassword = "password"
  
  // Timeout for long-running tests
  implicit val defaultPatience: PatienceConfig = 
    PatienceConfig(timeout = 5.minutes, interval = 500.millis)
  
  "StreamingHistoryTransform" should "process large datasets efficiently" taggedAs Production in {
    val testEvents = generateLargeStreamingEvents(PROD_TEST_SIZE)
    
    // Measure performance
    val startTime = System.nanoTime()
    
    val enriched = testEvents.map(StreamingHistoryTransform.enrichStreamingEvent)
    val userEventGroups = enriched.groupBy(e => (e.date, e.userId))
    val trackEventGroups = enriched.groupBy(e => (e.date, e.trackId))
    
    val userStats = userEventGroups.map { case ((date, userId), events) => 
      StreamingHistoryTransform.calculateUserListeningStats(events)
    }
    
    val trackStats = trackEventGroups.map { case ((date, trackId), events) => 
      StreamingHistoryTransform.calculateTrackPopularity(events)
    }
    
    val endTime = System.nanoTime()
    val duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime)
    
    // Assertions about performance and scale
    info(s"Processed ${testEvents.size} events in $duration ms")
    
    // Verify user stats
    userStats.foreach { stats =>
      stats.totalTracks should be > 0
      stats.totalTimeMs should be > 0L
    }
    
    // Verify track stats
    trackStats.foreach { stats =>
      stats.totalStreams should be > 0
      stats.uniqueListeners should be > 0
    }
    
    // Performance thresholds - adjust based on your environment
    duration should be < (PROD_TEST_SIZE * 2) // 2ms per event as a rough guideline
  }
  
  it should "handle edge cases in large datasets" taggedAs Production in {
    // Create a dataset with some edge cases
    val normalEvents = generateLargeStreamingEvents(PROD_TEST_SIZE / 2)
    
    // Add some edge cases - very short plays, very long plays, etc.
    val edgeCaseEvents = (1 to (PROD_TEST_SIZE / 2)).map { i =>
      val baseEvent = normalEvents(i % normalEvents.size)
      
      if (i % 5 == 0) {
        // Very short play (likely skipped)
        baseEvent.copy(msPlayed = 1000, completed = false)
      } else if (i % 5 == 1) {
        // Very long play (longer than track duration)
        baseEvent.copy(msPlayed = baseEvent.durationMs * 2, completed = true)
      } else if (i % 5 == 2) {
        // Zero play time
        baseEvent.copy(msPlayed = 0, completed = false)
      } else if (i % 5 == 3) {
        // Zero track duration (data error)
        baseEvent.copy(durationMs = 0)
      } else {
        // Normal but with explicit content
        baseEvent.copy(explicit = true)
      }
    }
    
    val allEvents = normalEvents ++ edgeCaseEvents
    val enriched = allEvents.map(StreamingHistoryTransform.enrichStreamingEvent)
    
    // Check that enrichments handle edge cases
    enriched.foreach { e =>
      // Completion ratio should be between 0 and 1
      e.completionRatio should be >= 0.0
      e.completionRatio should be <= 1.0
      
      // Date should be valid
      e.date should fullyMatch regex "\\d{4}-\\d{2}-\\d{2}"
      
      // Time of day should be one of the expected values
      e.timeOfDay should (be("morning") or be("afternoon") or be("evening") or be("night"))
    }
    
    // Group and calculate stats
    val userEventGroups = enriched.groupBy(e => (e.date, e.userId))
    val userStats = userEventGroups.map { case ((date, userId), events) => 
      StreamingHistoryTransform.calculateUserListeningStats(events)
    }
    
    // All user stats should be valid
    userStats.foreach { stat =>
      stat.totalTracks should be > 0
      stat.completionRate should be >= 0.0
      stat.completionRate should be <= 1.0
      
      // Check time breakdowns sum to total
      (stat.morningTracks + stat.afternoonTracks + 
       stat.eveningTracks + stat.nightTracks) should be(stat.totalTracks)
      (stat.weekdayTracks + stat.weekendTracks) should be(stat.totalTracks)
    }
  }
  
  it should "perform full pipeline execution with large dataset" taggedAs Production in {
    val testDate = "2023-01-15"
    val testEvents = generateLargeStreamingEvents(PROD_TEST_SIZE)
    
    val startTime = System.nanoTime()
    
    JobTest[StreamingHistoryTransform.type]
      .args("--date=" + testDate, "--env=test")
      .input(JdbcIO[StreamingEvent](mockRawDbConnUrl), testEvents)
      .output(JdbcIO[UserListeningStats](mockAnalyticsDbConnUrl)) { output =>
        output should not be empty
        
        // Check correct aggregation - should be fewer user records than input events
        output.size should be < testEvents.size
        
        // Check data quality
        output.foreach { stat =>
          stat.date should be(testDate)
          stat.totalTracks should be > 0
          stat.avgTrackDuration should be > 0.0
          
          // Ensure we have some non-zero values in our breakdown categories
          (stat.morningTracks + stat.afternoonTracks + 
           stat.eveningTracks + stat.nightTracks) should be(stat.totalTracks)
        }
      }
      .output(JdbcIO[TrackPopularity](mockAnalyticsDbConnUrl)) { output =>
        output should not be empty
        
        // Check correct aggregation - should be fewer track records than input events
        output.size should be < testEvents.size
        
        // Check data quality
        output.foreach { stat =>
          stat.date should be(testDate)
          stat.totalStreams should be > 0
          stat.uniqueListeners should be > 0
          stat.uniqueListeners should be <= stat.totalStreams
          
          // Ensure we have some non-zero values in our breakdown categories
          (stat.morningStreams + stat.afternoonStreams + 
           stat.eveningStreams + stat.nightStreams) should be(stat.totalStreams)
        }
      }
      .run()
    
    val endTime = System.nanoTime()
    val duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime)
    
    info(s"Processed ${testEvents.size} events through entire pipeline in $duration ms")
  }
} 