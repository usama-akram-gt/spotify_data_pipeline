package com.spotify.pipeline.transforms

import org.scalatest.Tag
import com.spotify.scio.testing.JobTest
import com.spotify.scio.jdbc.JdbcIO
import org.apache.beam.sdk.options.PipelineOptionsFactory

/**
 * Development tests for StreamingHistoryTransform - faster, less comprehensive
 */
class StreamingHistoryTransformDevSpec extends StreamingHistoryTransformSpecBase {
  
  // Tag for development tests
  object DevTest extends Tag("DevTest")
  
  // Test data size for dev tests
  val DEV_TEST_SIZE = 50
  
  // PostgreSQL connection mocks
  val mockRawDbConnUrl = "jdbc:postgresql://localhost:5432/spotify_raw"
  val mockAnalyticsDbConnUrl = "jdbc:postgresql://localhost:5432/spotify_analytics"
  val mockUser = "postgres"
  val mockPassword = "password"
  
  "StreamingHistoryTransform" should "enrich streaming events" taggedAs DevTest in {
    val inputEvents = generateStreamingEvents(10)
    val expected = enrichStreamingEvents(inputEvents)
    
    val actual = inputEvents.map(StreamingHistoryTransform.enrichStreamingEvent)
    
    actual.zip(expected).foreach { case (actual, expected) =>
      actual.eventId should be(expected.eventId)
      actual.userId should be(expected.userId)
      actual.trackId should be(expected.trackId)
      actual.date should be(expected.date)
      actual.timeOfDay should be(expected.timeOfDay)
      actual.isWeekend should be(expected.isWeekend)
      actual.completionRatio should be(expected.completionRatio +- 0.001)
    }
  }
  
  it should "calculate user listening statistics correctly" taggedAs DevTest in {
    val sampleDate = "2023-01-01"
    val userEvents = enrichStreamingEvents(generateStreamingEvents(20))
      .filter(_.date == sampleDate)
      .filter(_.userId == "user1")
    
    val userStats = StreamingHistoryTransform.calculateUserListeningStats(userEvents)
    
    userStats.date should be(sampleDate)
    userStats.userId should be("user1")
    userStats.totalTracks should be > 0
    userStats.totalTimeMs should be > 0L
    userStats.avgTrackDuration should be > 0.0
    
    // Time of day stats should add up to total tracks
    (userStats.morningTracks + userStats.afternoonTracks + 
     userStats.eveningTracks + userStats.nightTracks) should be(userStats.totalTracks)
    
    // Day type stats should add up to total tracks
    (userStats.weekdayTracks + userStats.weekendTracks) should be(userStats.totalTracks)
  }
  
  it should "calculate track popularity metrics correctly" taggedAs DevTest in {
    val sampleDate = "2023-01-01"
    val trackEvents = enrichStreamingEvents(generateStreamingEvents(20))
      .filter(_.date == sampleDate)
      .filter(_.trackId == "track1")
    
    val trackPopularity = StreamingHistoryTransform.calculateTrackPopularity(trackEvents)
    
    trackPopularity.date should be(sampleDate)
    trackPopularity.trackId should be("track1")
    trackPopularity.totalStreams should be(trackEvents.size)
    trackPopularity.uniqueListeners should be <= trackEvents.size
    trackPopularity.uniqueListeners should be > 0
    trackPopularity.avgCompletionRate should be <= 1.0
    trackPopularity.avgCompletionRate should be >= 0.0
    
    // Time of day stats should add up to total streams
    (trackPopularity.morningStreams + trackPopularity.afternoonStreams + 
     trackPopularity.eveningStreams + trackPopularity.nightStreams) should be(trackPopularity.totalStreams)
    
    // Day type stats should add up to total streams
    (trackPopularity.weekdayStreams + trackPopularity.weekendStreams) should be(trackPopularity.totalStreams)
  }
  
  it should "perform end-to-end pipeline processing" taggedAs DevTest in {
    val testDate = "2023-01-15"
    val testEvents = generateStreamingEvents(DEV_TEST_SIZE)
    
    JobTest[StreamingHistoryTransform.type]
      .args("--date=" + testDate, "--env=test")
      .input(JdbcIO[StreamingEvent](mockRawDbConnUrl), testEvents)
      .output(JdbcIO[UserListeningStats](mockAnalyticsDbConnUrl)) { output =>
        output should not be empty
        
        // Verify that there's at least one processed record per user
        val userGroups = output.groupBy(_.userId)
        userGroups.size should be > 0
        
        // Check that all stats have the correct date
        output.foreach(_.date should be(testDate))
      }
      .output(JdbcIO[TrackPopularity](mockAnalyticsDbConnUrl)) { output =>
        output should not be empty
        
        // Verify that there's at least one processed record per track
        val trackGroups = output.groupBy(_.trackId)
        trackGroups.size should be > 0
        
        // Check that all stats have the correct date
        output.foreach(_.date should be(testDate))
      }
      .run()
  }
} 