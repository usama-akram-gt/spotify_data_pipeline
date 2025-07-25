package com.spotify.pipeline.transforms

import com.spotify.scio.testing._
import com.spotify.scio.jdbc._
import org.apache.beam.sdk.io.jdbc.JdbcIO
import org.apache.beam.sdk.values.PCollection
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, PreparedStatement, ResultSet}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._

// Test case for demonstrating JDBC interactions in Scio
class ScioJdbcTest extends AnyFlatSpec with ScioIOSpec with Matchers {

  // Mock data structures
  case class TestRecord(id: String, name: String, value: Int)

  // This test demonstrates how to test a Scio pipeline that reads from JDBC
  "A Scio job" should "read from JDBC and transform data" in {
    val inputs = Seq(
      TestRecord("1", "record1", 100),
      TestRecord("2", "record2", 200),
      TestRecord("3", "record3", 300)
    )

    runWithMockedJdbc(
      inputs,
      read = rs => TestRecord(
        rs.getString("id"),
        rs.getString("name"),
        rs.getInt("value")
      )
    ) { (sc, mockRead) =>
      // This is what a pipeline would typically do
      val data = sc.customInput(mockRead)
      
      // Simple transformation
      val results = data
        .map(record => (record.id, record.value * 2))
        .map { case (id, value) => s"$id:$value" }
      
      // Assert results
      results should containInAnyOrder(Seq(
        "1:200",
        "2:400",
        "3:600"
      ))
    }
  }

  // Helper method to run a test with mocked JDBC source
  def runWithMockedJdbc[T](
    mockData: Seq[T],
    read: ResultSet => T
  )(testFn: (ScioContext, JdbcRead[T]) => Unit): Unit = {
    // Create mock JDBC components
    val mockResultSet = createMockResultSet(mockData, read)
    val mockConnection = createMockConnection(mockResultSet)
    
    // Create a mock JdbcRead object
    val mockRead = JdbcRead[T](
      JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/test"),
      "SELECT * FROM test_table",
      read
    ) {
      override protected def getConnection = mockConnection
    }
    
    // Run the test with a ScioContext
    JobTest[StreamingHistoryTransform.type]
      .args("--testMode=true")
      .run { sc =>
        testFn(sc, mockRead)
      }
  }

  // Helper to create a mock ResultSet with the provided data
  private def createMockResultSet[T](data: Seq[T], read: ResultSet => T): ResultSet = {
    val mockResultSet = mock(classOf[ResultSet])
    
    // Set up the mock to return values from data sequentially
    var callCount = -1
    when(mockResultSet.next()).thenAnswer(_ => {
      callCount += 1
      callCount < data.size
    })
    
    // Add mock behavior for common column types
    when(mockResultSet.getString(anyString())).thenAnswer(invocation => {
      val columnName = invocation.getArgument[String](0)
      // In a real implementation, you would extract the actual value from your data
      // This is simplified for the example
      s"mock_$columnName${callCount}"
    })
    
    when(mockResultSet.getInt(anyString())).thenAnswer(invocation => {
      val columnName = invocation.getArgument[String](0)
      // Similarly simplified
      callCount * 100
    })
    
    mockResultSet
  }
  
  // Helper to create a mock Connection
  private def createMockConnection(resultSet: ResultSet): Connection = {
    val mockConnection = mock(classOf[Connection])
    val mockStatement = mock(classOf[PreparedStatement])
    
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement)
    when(mockStatement.executeQuery()).thenReturn(resultSet)
    
    mockConnection
  }
} 