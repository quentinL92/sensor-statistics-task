import java.io.{BufferedWriter, File, FileWriter, FilenameFilter}
import scala.util.Random

val outputDirName: String = "D:\\Dev\\Scala\\Luxoft\\src\\main\\scala\\app"
val outputDirectory = new File(outputDirName)
val nextFileName: String = outputDirName + "\\leader-" + (outputDirectory.listFiles(new FilenameFilter {
  override def accept(dir: File, name: String) = name.endsWith(".csv")
}).map(_.getName.stripSuffix(".csv").split('-').last.toInt).max + 1) + ".csv"
val outputFile: File = new File(nextFileName)
val bw = new BufferedWriter(new FileWriter(outputFile))

def humidityValue(): String = {
  val value = Random.nextInt(120)
  if (value > 100) "NaN"
  else value.toString
}
def writeLine(line: String): Unit = {
  bw.write(line)
  bw.newLine()
}

writeLine("sensor-id,humidity")
for {
  _ <- 0 until 2
  innerIteration <- 100 to 0 by -1
} writeLine(s"s999,$innerIteration")

bw.close()