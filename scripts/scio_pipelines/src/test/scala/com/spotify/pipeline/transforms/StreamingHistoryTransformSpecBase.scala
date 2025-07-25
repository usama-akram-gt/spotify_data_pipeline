package com.spotify.pipeline.transforms

import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.spotify.scio.testing.{JobTest, PipelineSpec, TestValidationOptions}
import com.spotify.scio.io.TextIO
import com.spotify.scio.values.SCollection
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Base test specification for Scio tests with development and production markers
 */
trait StreamingHistoryTransformSpecBase extends AnyFlatSpec with Matchers with BeforeAndAfterEach with PipelineSpec {
  
  // Test data generators
  def generateStreamingEvents(count: Int = 10): Seq[StreamingEvent] = {
    val users = Seq("user1", "user2", "user3", "user4", "user5")
    val tracks = Seq("track1", "track2", "track3", "track4", "track5")
    val artists = Seq("artist1", "artist2", "artist3", "artist4", "artist5")
    
    (1 to count).map { i =>
      val userId = users(i % users.length)
      val trackId = tracks(i % tracks.length)
      val artistId = artists(i % artists.length)
      val date = LocalDateTime.of(2023, 1, (i % 28) + 1, (i % 24), 0)
      
      StreamingEvent(
        eventId = s"event-$i",
        userId = userId,
        trackId = trackId,
        artistId = artistId,
        playedAt = date.format(DateTimeFormatter.ISO_DATE_TIME),
        msPlayed = 180000 + (i * 1000),
        trackName = s"Track $trackId",
        artistName = s"Artist $artistId",
        completed = i % 3 != 0, // Every third track is not completed
        durationMs = 200000 + (i * 500),
        explicit = i % 5 == 0,
        trackPopularity = 50 + (i % 50),
        genres = s"""["genre${i % 5 + 1}", "genre${(i + 3) % 5 + 1}"]""",
        artistPopularity = 60 + (i % 40)
      )
    }
  }
  
  def generateLargeStreamingEvents(count: Int = 1000): Seq[StreamingEvent] = {
    // For production tests with larger datasets
    generateStreamingEvents(count)
  }
  
  // Helper to create enriched streaming events
  def enrichStreamingEvents(events: Seq[StreamingEvent]): Seq[EnrichedStreamingEvent] = {
    events.map { event =>
      val date = LocalDateTime.parse(event.playedAt, DateTimeFormatter.ISO_DATE_TIME)
      val hour = date.getHour
      val dayOfWeek = date.getDayOfWeek.getValue
      val isWeekend = dayOfWeek >= 6
      
      val timeOfDay = hour match {
        case h if h >= 5 && h < 12 => "morning"
        case h if h >= 12 && h < 17 => "afternoon"
        case h if h >= 17 && h < 22 => "evening"
        case _ => "night"
      }
      
      val completionRatio = if (event.durationMs > 0) {
        Math.min(event.msPlayed.toDouble / event.durationMs.toDouble, 1.0)
      } else 0.0
      
      EnrichedStreamingEvent(
        eventId = event.eventId,
        userId = event.userId,
        trackId = event.trackId,
        artistId = event.artistId,
        playedAt = event.playedAt,
        date = date.toLocalDate.toString,
        hour = hour,
        timeOfDay = timeOfDay,
        isWeekend = isWeekend,
        msPlayed = event.msPlayed,
        trackName = event.trackName,
        artistName = event.artistName,
        durationMs = event.durationMs,
        completionRatio = completionRatio,
        completed = event.completed,
        explicit = event.explicit,
        trackPopularity = event.trackPopularity,
        genres = event.genres,
        artistPopularity = event.artistPopularity
      )
    }
  }
} 