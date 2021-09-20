package streams

import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import constants.Constants.failureValue
import models.SensorRecord


class FlowsSpec extends StreamsFlatSpec {

  val (pub, sub) = TestSource
    .probe[String]
    .via(Flows.csvLineToRecordFlow)
    .toMat(TestSink[(String, SensorRecord)]())(Keep.both)
    .run()

  "Flows#csvLineToRecordFlow" should
    "transform each failed measurements into a tuple (id, record) with an undefined humidity" in {
    sub.request(4)
    pub.sendNext(s"s1,$failureValue")
    pub.sendNext(s"s20,$failureValue")
    pub.sendNext(s"s300,$failureValue")
    pub.sendNext(s"s1,$failureValue")
    assert(sub.expectNext() == undefinedRecord("s1"))
    assert(sub.expectNext() == undefinedRecord("s20"))
    assert(sub.expectNext() == undefinedRecord("s300"))
    assert(sub.expectNext() == undefinedRecord("s1"))
  }

  it should "transform each non-failed measurements into a tuple (id, record) with a defined humidity" in {
    sub.request(4)
    pub.sendNext("s1,100")
    pub.sendNext("s2,39")
    pub.sendNext("s3,50")
    pub.sendNext("s1,0")
    assert(sub.expectNext() == "s1" -> SensorRecord("s1", Some(100)))
    assert(sub.expectNext() == "s2" -> SensorRecord("s2", Some(39)))
    assert(sub.expectNext() == "s3" -> SensorRecord("s3", Some(50)))
    assert(sub.expectNext() == "s1" -> SensorRecord("s1", Some(0)))
  }

  it should "correctly transform each measurements" in {
    sub.request(8)
    pub.sendNext(s"s1,$failureValue")
    pub.sendNext("s2,39")
    pub.sendNext(s"s300,$failureValue")
    pub.sendNext("s1,100")
    pub.sendNext("s3,50")
    pub.sendNext(s"s20,$failureValue")
    pub.sendNext(s"s1,$failureValue")
    pub.sendNext("s1,0")
    assert(sub.expectNext() == undefinedRecord("s1"))
    assert(sub.expectNext() == "s2" -> SensorRecord("s2", Some(39)))
    assert(sub.expectNext() == undefinedRecord("s300"))
    assert(sub.expectNext() == "s1" -> SensorRecord("s1", Some(100)))
    assert(sub.expectNext() == "s3" -> SensorRecord("s3", Some(50)))
    assert(sub.expectNext() == undefinedRecord("s20"))
    assert(sub.expectNext() == undefinedRecord("s1"))
    assert(sub.expectNext() == "s1" -> SensorRecord("s1", Some(0)))
  }
}
