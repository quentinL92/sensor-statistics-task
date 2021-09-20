package streams

import akka.stream.scaladsl.Sink
import models.{SensorRecord, SensorStat}

import scala.concurrent.Future

object Sinks {
  val recordsToStatsSink: Sink[(String, SensorRecord), Future[Map[String, SensorStat]]] =
    Sink.fold[Map[String, SensorStat], (String, SensorRecord)](Map.empty[String, SensorStat]) {
      case (acc, (id, sensorRecord)) => acc.updatedWith(id) {
        case Some(sensorStat) => Some(sensorStat.ingest(sensorRecord))
        case None             => Some(SensorStat(sensorRecord))
      }
    }
}
