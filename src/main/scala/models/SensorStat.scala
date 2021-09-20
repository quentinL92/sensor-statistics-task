package models

import constants.Constants

case class SensorStat(id: String, stat: Option[Stat], numberOfFailures: Int = 0) {
  def ingest(sensorRecord: SensorRecord): SensorStat = sensorRecord.humidity match {
    case None                => copy(numberOfFailures = numberOfFailures + 1)
    case Some(humidityValue) => stat match {
      case None =>
        copy(stat = Some(Stat(humidityValue, humidityValue, humidityValue)))

      case Some(Stat(min, avg, max, numberOfRecords)) =>
        val totalNumberOfRecords = numberOfRecords + 1
        copy(stat = Some(Stat(
          List(min, humidityValue).min,
          (avg * numberOfRecords + humidityValue) / totalNumberOfRecords,
          List(max, humidityValue).max,
          totalNumberOfRecords
        )))
    }
  }

  def numberOfProcessedMeasurements: Int = numberOfFailures + stat.map(_.numberOfRecords).getOrElse(0)

  override def toString: String = {
    def getOrNaN(f: Stat => Int): String = stat.map(f(_).toString).getOrElse(Constants.failureValue)

    s"$id,${getOrNaN(_.min)},${getOrNaN(_.avg.toInt)},${getOrNaN(_.max)}"
  }
}

object SensorStat {
  def apply(sensorRecord: SensorRecord): SensorStat = SensorStat(
    sensorRecord.id,
    sensorRecord.humidity.map(humidityValue => Stat(humidityValue, humidityValue, humidityValue)),
    if (sensorRecord.humidity.isDefined) 0 else 1
  )

  object Implicits {
    implicit val orderingSensorStat: Ordering[SensorStat] = new SensorStatOrdering
  }

  class SensorStatOrdering extends Ordering[SensorStat] {
    override def compare(x: SensorStat, y: SensorStat): Int = (x.stat, y.stat) match {
      case (None, None)                                           => 0
      case (Some(_), None)                                        => -1
      case (None, Some(_))                                        => 1
      case (Some(Stat(_, xAvg, _, _)), Some(Stat(_, yAvg, _, _))) => yAvg.compare(xAvg)
    }
  }
}
