package streams

import akka.NotUsed
import akka.stream.scaladsl.Flow
import constants.Constants.failureValue
import models.SensorRecord

object Flows {

  val csvLineToRecordFlow: Flow[String, (String, SensorRecord), NotUsed] =
    Flow[String]
      .map {
        _.split(',') match {
          case Array(id, humidity, _*) if humidity == failureValue => id -> SensorRecord(id, None)
          case Array(id, humidity, _*)                             => id -> SensorRecord(id, Some(humidity.toInt))
        }
      }
}
