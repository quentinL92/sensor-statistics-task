package streams

import akka.stream.scaladsl.Sink
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import app.Main

import java.io.File

class SourcesSpec extends StreamsFlatSpec {
  def getTestFile(fileName: String): File = new File(s"./src/test/scala/data/$fileName.csv")

  val testSink: Sink[String, TestSubscriber.Probe[String]] = TestSink[String]()

  "Sources#sourceFromFile" should "separate all lines in the file and return them all one by one expecting the " +
    "first one as a String" in {
    Sources
      .sourceFromFile(getTestFile("leader-1"))
      .runWith(testSink)
      .request(3)
      .expectNext("s1,10", "s2,88", "s1,NaN")
      .expectComplete()

    Sources
      .sourceFromFile(getTestFile("leader-2"))
      .runWith(testSink)
      .request(4)
      .expectNext("s2,80", "s3,NaN", "s2,78", "s1,98")
      .expectComplete()
  }

  it should "not return any String for blank files or files with only headers but no records" in {
    Sources
      .sourceFromFile(getTestFile("header_only"))
      .runWith(testSink)
      .request(1)
      .expectComplete()

    Sources
      .sourceFromFile(getTestFile("blank"))
      .runWith(testSink)
      .request(1)
      .expectComplete()
  }
}
