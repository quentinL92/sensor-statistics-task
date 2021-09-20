import java.io.{BufferedWriter, File, FileWriter, FilenameFilter}
import scala.util.Random

val outputDirName: String = "D:\\Dev\\Scala\\Luxoft\\src\\main\\scala\\app"
val outputDirectory = new File(outputDirName)
val nextFileName: String = outputDirName + "\\leader-" + (outputDirectory.listFiles(new FilenameFilter {
  override def accept(dir: File, name: String) = name.endsWith(".csv")
}).map(_.getName.stripSuffix(".csv").split('-').last.toInt).max + 1) + ".csv"
val outputFile: File = new File(nextFileName)
val bw = new BufferedWriter(new FileWriter(outputFile))

val numberOfLines: Int = 270023410
val numberOfSensors: Int = 4595
def sensorId(): String = s"s${Random.nextInt(numberOfSensors) + 1}"
def humidityValue(): String = {
  lazy val shouldBeStrongValue = Random.nextInt(8000) == 0
  val value = Random.nextInt(120)
  if (value > 100) "NaN"
  else if (value < 15) (value + (if (shouldBeStrongValue) 0 else 10)).toString
  else if (value > 85) (value - (if (shouldBeStrongValue) 0 else 10)).toString
  else value.toString
}
def writeLine(line: String): Unit = {
  bw.write(line)
  bw.newLine()
}

writeLine("sensor-id,humidity")
(0 until numberOfLines).map { _ =>
  writeLine(s"${sensorId()},${humidityValue()}")
}
bw.close()