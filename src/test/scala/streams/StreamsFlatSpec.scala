package streams

import akka.actor.ActorSystem
import models.SensorRecord
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}

trait StreamsSpec {
  implicit val system: ActorSystem = ActorSystem("test")
  def undefinedRecord(id: String): (String, SensorRecord) = id -> SensorRecord(id, None)
  def definedRecord(id: String, humidityValue: Int): (String, SensorRecord) = id -> SensorRecord(id, Some(humidityValue))
}

trait StreamsFlatSpec extends AnyFlatSpec with StreamsSpec
trait StreamsAsyncSpec extends AsyncFlatSpec with StreamsSpec
