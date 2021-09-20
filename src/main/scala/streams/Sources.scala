package streams

import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString

import java.io.File
import java.nio.file.Paths
import scala.concurrent.Future

object Sources {
  def sourceFromFile(file: File): Source[String, Future[IOResult]] = FileIO
    .fromPath(Paths.get(file.getAbsolutePath))
    .via(Framing.delimiter(ByteString(System.lineSeparator()), maximumFrameLength = 100, allowTruncation = true))
    .map(_.utf8String)
    .drop(1)
}
