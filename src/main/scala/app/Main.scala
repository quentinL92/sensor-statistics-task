package app

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import models.SensorStat.Implicits.orderingSensorStat
import models._
import streams.Flows.csvLineToRecordFlow
import streams.Sinks.recordsToStatsSink
import streams.Sources.sourceFromFile

import java.io.{File, FilenameFilter}
import scala.concurrent._
import scala.util.{Failure, Success}

object Main extends App {

  implicit val system: ActorSystem = ActorSystem("QuickStart")
  implicit val ec: ExecutionContext = system.dispatcher

  /**
   * Main process that goes through the directory passed, compute the stats of all the csv files in it
   * and display the result.
   * @param directory the directory where the csv files are located
   */
  def processDirectory(directory: File): Unit = {
    val sources = directory
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = name.endsWith(".csv")
      })
      .toList

    Source
      .combine(Source.empty[String], Source.empty[String], sources.map(sourceFromFile): _*)(Merge(_))
      .async
      .via(csvLineToRecordFlow)
      .toMat(recordsToStatsSink)(Keep.right)
      .run()
      .onComplete {
        case Failure(exception) =>
          println(s"Failure with $exception")
          system.terminate()

        case Success(sensorStats) =>
          def printStats(humidityResults: Vector[SensorStat]): Unit = {
            println(s"Num of processed files: ${sources.size}")
            println(s"num of processed measurements: ${humidityResults.map(_.numberOfProcessedMeasurements).sum}")
            println(s"Num of failed measurements: ${humidityResults.map(_.numberOfFailures).sum}")
            println()
            println("Sensors with highest avg humidity:")
            println()
            println("sensor-id,min,avg,max")
            humidityResults.foreach(println)
          }

          printStats(sensorStats.values.toVector.sorted)
          system.terminate()
      }

  }

  lazy val directory = new File(args.head)

  args.headOption match {
    case None =>
      System.err.println("No arguments were found. Please provide a directory as the only argument.")
      sys.exit(1)

    case Some(_) if directory.isDirectory =>
      processDirectory(directory)

    case Some(value) =>
      System.err.println(s"$value is not a directory.")
      sys.exit(2)
  }

}
