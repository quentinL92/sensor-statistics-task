package streams

import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.TestSource
import models.{SensorRecord, SensorStat, Stat}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class SinksSpec extends StreamsAsyncSpec with ScalaFutures {
  val testPub: Source[(String, SensorRecord), TestPublisher.Probe[(String, SensorRecord)]] =
    TestSource.probe[(String, SensorRecord)]

  def pubFuture: (TestPublisher.Probe[(String, SensorRecord)], Future[Map[String, SensorStat]]) =
    testPub.toMat(Sinks.recordsToStatsSink)(Keep.both).run()

  "Sinks#recordsToStatsSink" should
    "transform all unique failed measurement (record with undefined humidity) into an undefined stat with 1 failure" in {
    val (pub, future: Future[Map[String, SensorStat]]) = pubFuture
    pub.sendNext(undefinedRecord("s1"))
    pub.sendNext(undefinedRecord("s2"))
    pub.sendNext(undefinedRecord("s3"))
    pub.sendNext(undefinedRecord("s4"))
    pub.sendComplete()

    whenReady(future)(r => assert(r == Map(
      "s1" -> SensorStat("s1", None, 1),
      "s2" -> SensorStat("s2", None, 1),
      "s3" -> SensorStat("s3", None, 1),
      "s4" -> SensorStat("s4", None, 1)
    )))
  }

  it should "stack failed measurements on a same sensor in an undefined stat with the correct number of failures" in {
    val (pub, future: Future[Map[String, SensorStat]]) = pubFuture

    pub.sendNext(undefinedRecord("s1"))
    pub.sendNext(undefinedRecord("s1"))
    pub.sendNext(undefinedRecord("s2"))
    pub.sendNext(undefinedRecord("s2"))
    pub.sendNext(undefinedRecord("s1"))
    pub.sendComplete()

    whenReady(future)(r => assert(r == Map(
      "s1" -> SensorStat("s1", None, 3),
      "s2" -> SensorStat("s2", None, 2)
    )))
  }

  it should "compute the correct extrema and average of multiple records on the same sensor and keep track " +
    "of the measurements" in {
    val (pub, future: Future[Map[String, SensorStat]]) = pubFuture

    pub.sendNext(definedRecord("s1", 50))
    pub.sendNext(definedRecord("s1", 100))
    pub.sendNext(definedRecord("s1", 0))
    pub.sendNext(definedRecord("s1", 25))
    pub.sendNext(definedRecord("s1", 75))
    pub.sendComplete()

    whenReady(future)(r => assert(r == Map(
      "s1" -> SensorStat("s1", Some(Stat(0, 50, 100, 5)))
    )))
  }

  it should "compute the correct extrema and average of multiple records with the same value on the same sensor and " +
    "keep track of the measurements" in {
    val (pub0, future0: Future[Map[String, SensorStat]]) = pubFuture

    pub0.sendNext(definedRecord("s1", 0))
    pub0.sendNext(definedRecord("s1", 0))
    pub0.sendNext(definedRecord("s1", 0))
    pub0.sendNext(definedRecord("s1", 0))
    pub0.sendNext(definedRecord("s1", 0))
    pub0.sendComplete()

    whenReady(future0)(r => assert(r == Map(
      "s1" -> SensorStat("s1", Some(Stat(0, 0, 0, 5)))
    )))

    val (pub100, future100: Future[Map[String, SensorStat]]) = pubFuture

    pub100.sendNext(definedRecord("s1", 100))
    pub100.sendNext(definedRecord("s1", 100))
    pub100.sendNext(definedRecord("s1", 100))
    pub100.sendNext(definedRecord("s1", 100))
    pub100.sendNext(definedRecord("s1", 100))
    pub100.sendComplete()

    whenReady(future100)(r => assert(r == Map(
      "s1" -> SensorStat("s1", Some(Stat(100, 100, 100, 5)))
    )))
  }

  it should
    "compute the correct extrema and average of mutliple records on the same sensor and keep track of both the " +
      "failed and succeeded measurements" in {
    val (pub, future) = pubFuture

    pub.sendNext(definedRecord("s1", 33))
    pub.sendNext(definedRecord("s1", 66))
    pub.sendNext(undefinedRecord("s1"))
    pub.sendNext(definedRecord("s1", 50))
    pub.sendNext(definedRecord("s1", 100))
    pub.sendNext(undefinedRecord("s1"))
    pub.sendNext(undefinedRecord("s1"))
    pub.sendComplete()

    whenReady(future) (r => assert(r == Map(
      "s1" -> SensorStat("s1", Some(Stat(33, 62.25, 100, 4)), 3)
    )))

  }

  it should
    "compute the correct extrema and average of mutliple records on different sensors and keep track of both the " +
      "failed and succeeded measurements" in {
    val (pubFollowingRecords, futureFollowingRecords) = pubFuture

    pubFollowingRecords.sendNext(definedRecord("s1", 33))
    pubFollowingRecords.sendNext(definedRecord("s1", 66))
    pubFollowingRecords.sendNext(undefinedRecord("s1"))
    pubFollowingRecords.sendNext(definedRecord("s1", 50))
    pubFollowingRecords.sendNext(definedRecord("s1", 100))
    pubFollowingRecords.sendNext(undefinedRecord("s1"))
    pubFollowingRecords.sendNext(undefinedRecord("s1"))

    pubFollowingRecords.sendNext(undefinedRecord("s2"))
    pubFollowingRecords.sendNext(definedRecord("s2", 20))
    pubFollowingRecords.sendNext(definedRecord("s2", 40))
    pubFollowingRecords.sendNext(undefinedRecord("s2"))
    pubFollowingRecords.sendNext(definedRecord("s2", 60))
    pubFollowingRecords.sendNext(definedRecord("s2", 20))
    pubFollowingRecords.sendNext(undefinedRecord("s2"))
    pubFollowingRecords.sendNext(undefinedRecord("s2"))
    pubFollowingRecords.sendNext(definedRecord("s2", 80))
    pubFollowingRecords.sendComplete()

    whenReady(futureFollowingRecords) (r => assert(r == Map(
      "s1" -> SensorStat("s1", Some(Stat(33, 62.25, 100, 4)), 3),
      "s2" -> SensorStat("s2", Some(Stat(20, 44, 80, 5)), 4)
    )))

    val (pubMixedRecords, futureMixedRecords) = pubFuture

    pubMixedRecords.sendNext(undefinedRecord("s2"))
    pubMixedRecords.sendNext(undefinedRecord("s2"))
    pubMixedRecords.sendNext(definedRecord("s1", 66))
    pubMixedRecords.sendNext(undefinedRecord("s1"))
    pubMixedRecords.sendNext(definedRecord("s1", 50))
    pubMixedRecords.sendNext(definedRecord("s2", 40))
    pubMixedRecords.sendNext(undefinedRecord("s2"))
    pubMixedRecords.sendNext(definedRecord("s2", 60))
    pubMixedRecords.sendNext(undefinedRecord("s1"))
    pubMixedRecords.sendNext(definedRecord("s2", 20))
    pubMixedRecords.sendNext(undefinedRecord("s2"))
    pubMixedRecords.sendNext(definedRecord("s1", 33))
    pubMixedRecords.sendNext(definedRecord("s2", 80))
    pubMixedRecords.sendNext(definedRecord("s1", 100))
    pubMixedRecords.sendNext(definedRecord("s2", 20))
    pubMixedRecords.sendNext(undefinedRecord("s1"))
    pubMixedRecords.sendComplete()

    whenReady(futureMixedRecords) (r => assert(r == Map(
      "s2" -> SensorStat("s2", Some(Stat(20, 44, 80, 5)), 4),
      "s1" -> SensorStat("s1", Some(Stat(33, 62.25, 100, 4)), 3)
    )))

  }

}
