
import java.net.URL
import org.htmlcleaner.HtmlCleaner
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.receiver._
import org.apache.spark.storage._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkStreamingHtmlParser {
    def main(args: Array[String]): Unit = {
        try{
            println("*** PROGRAM STARTED ***")

            // suppress the annoying loggings from Spark (https://stackoverflow.com/questions/25193488/how-to-turn-off-info-logging-in-pyspark)
            Logger.getLogger("org").setLevel(Level.OFF)
            Logger.getLogger("akka").setLevel(Level.OFF)

            //val etfh = new ExtractTextFromHtml("http://www.zappos.com/josef-seibel-ruth-03", "priceSlot")
            //println(etfh.extract())

            val str1 = "http://www.amazon.com/LG-D820-Unlocked-Certified-Refurbished/dp/B017ROJ2NC|priceblock_ourprice"
            val str2 = "http://www.amazon.com/Huawei-Nexus-6P-Smartphone-32/dp/B019TWO6WM|priceblock_ourprice"
            val str3 = "http://www.amazon.com/Samsung-Galaxy-Factory-Unlocked-Phone/dp/B01CJU9126|priceblock_ourprice"
            val str4 = "http://www.zappos.com/josef-seibel-ruth-03|priceSlot"
            val str5 = "http://www.zappos.com/nike-flex-fury-2~2|priceSlot"
            val str6 = "http://www.zappos.com/kork-ease-myrna-2-0|priceSlot"

            val ssc = new StreamingContext(new SparkConf().setMaster("local[4]").setAppName("HtmlParser"), Seconds(10))
            val myStream = ssc.receiverStream(new HtmlReceiver(List(str1, str2, str3, str4, str5, str6)))
            myStream.print()

            ssc.start()
            ssc.awaitTermination()

            println("*** PROGRAM SUCCEEDED ***")
        }
        catch {
            case e: Exception => println("*** PROGRAM EXCEPTION: " + e.getMessage() + " ***");
        }
        finally {
            println("*** PROGRAM ENDED ***")
        }
    }
}

class ExtractTextFromHtml(url: String, id: String){
    val USER_AGENT = "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36"

    def extract(): String = {
        var result = ""

        val urlUrl = new URL(url)
        val lConn = urlUrl.openConnection()
        lConn.setRequestProperty("User-Agent", USER_AGENT)
        lConn.connect()

        val cleaner = new HtmlCleaner()
        val rootNode = cleaner.clean(lConn.getInputStream())

        val eleCol = rootNode.getElementsByAttValue("id", id, true, true)
        if (eleCol != null && eleCol.length > 0){
            result = eleCol(0).getText.toString.trim        // get text from children also
        }

        return result
    }
}

class HtmlReceiver(hosts: List[String]) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
    def onStart(): Unit ={
        new Thread("Socket Receiver"){
            override def run(): Unit ={
                receive()
            }
        }.start()
    }

    def onStop(): Unit ={

    }

    private def receive(): Unit ={
        try{
            while(!isStopped){
                for (host <- hosts){
                    val arr = host.split("\\|")
                    val etfh = new ExtractTextFromHtml(arr(0), arr(1))  // "priceblock_ourprice"
                    store(host + "|" + etfh.extract())                  // a single string
                    Thread.sleep(500)                                   // no need to hurry
                }
            }

            restart("Trying to connect again")
        }
        catch {
            case e: Exception => restart("Error receiving data", e)
        }
    }
}

/*
Reference:
http://alvinalexander.com/scala/scala-html-parsing
https://github.com/cecol/testSample/blob/master/src/main/scala/test/app/xpath/Xpath.scala
 */
