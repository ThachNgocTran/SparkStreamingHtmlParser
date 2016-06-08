
import java.net.URL
import java.util.concurrent.{Callable, ExecutorService, Executors, TimeUnit}

import org.htmlcleaner.HtmlCleaner
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.receiver._
import org.apache.spark.storage._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.concurrent.Future
import scala.util.matching.Regex

import scala.collection.mutable.MutableList

object SparkStreamingHtmlParser {
    def main(args: Array[String]): Unit = {
        try{
            println("*** PROGRAM STARTED ***")

            // suppress the annoying loggings from Spark (https://stackoverflow.com/questions/25193488/how-to-turn-off-info-logging-in-pyspark)
            Logger.getLogger("org").setLevel(Level.OFF)
            Logger.getLogger("akka").setLevel(Level.OFF)

            //val etfh = new ExtractTextFromHtml("http://localhost:8000", "myprice")
            //println(etfh.call())

            val str1 = "https://www.amazon.com/LG-D820-Unlocked-Certified-Refurbished/dp/B017ROJ2NC|priceblock_ourprice"
            val str2 = "https://www.amazon.com/Huawei-Nexus-6P-Smartphone-32/dp/B019TWO6WM|priceblock_ourprice"
            val str3 = "https://www.amazon.com/Samsung-Galaxy-Factory-Unlocked-Phone/dp/B01CJU9126|priceblock_ourprice"
            val str4 = "http://www.zappos.com/josef-seibel-ruth-03|priceSlot"
            val str5 = "http://www.zappos.com/nike-flex-fury-2~2|priceSlot"
            val str6 = "http://www.zappos.com/kork-ease-myrna-2-0|priceSlot"
            val str7 = "http://www.zappos.com/kork-ease-ava-2-0-black|priceSlot"
            val str8 = "http://localhost:8000|myprice"

            val ssc = new StreamingContext(new SparkConf().setMaster("local[4]").setAppName("HtmlParser"), Seconds(10))
            val myStream = ssc.receiverStream(new HtmlReceiver(List(str1, str2, str3, str4, str5, str6, str7, str8)))
            //myStream.print()

            // Update the state of keys.
            // First, convert to (key, value) pairs.
            val pairs = myStream.map({str =>
                                        val arr = str.split("\\|")
                                        (arr(0) + "|" + arr(1), arr(2))
                                    })                              // (key, value) = ("http://www.zappos.com/nike-flex-fury-2~2|priceSlot", "$123.99")

            // Now update them
            val runningNumbers = pairs.updateStateByKey[String](updateFunction _)

            // Print the states
            runningNumbers.print()

            // Checkpoint directory for recovery. ==> a must-have for stateful streaming.
            ssc.checkpoint("checkpointdirectory")
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

    /*
    To maintain the lowest prices ever encountered during the runtime. Real prices can fluctuate over time.
     */
    def updateFunction(newValues: Seq[String], runningNumber: Option[String]): Option[String] = {
        val pattern = new Regex("[+-]?[0-9.,]+")    // match real numbers only
        var runValStr: String = runningNumber.getOrElse("-1")
        var runValNum: Double = pattern.findFirstIn(runningNumber.getOrElse("-1")).get.replace(",", "").toDouble
        if (runValNum < 0)                          // for easier comparison with initial value
            runValNum = Double.MaxValue

        for(newValStr <- newValues){
            val extracted = pattern.findFirstIn(newValStr)  // maybe errors
            if (extracted != None){
                val newValNum = extracted.get.replace(",", "").toDouble
                if (newValNum < runValNum){
                    runValNum = newValNum
                    runValStr = newValStr
                }
            }
        }

        //println("Old Value: %s; New Value: %s".format(runningNumber.getOrElse("-1"), runValStr))

        Some(runValStr) // use Option[String] rather than Optionp[Double] because of $ and €
    }
}

//class ExtractTextFromHtml(url: String, id: String) {
class ExtractTextFromHtml(url: String, id: String) extends Callable[String] {
    val USER_AGENT = "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36"

    //def extract(): String = {
    def call(): String = {
        var result = ""

        try{
            val urlUrl = new URL(url)
            val lConn = urlUrl.openConnection()
            lConn.setRequestProperty("User-Agent", USER_AGENT)
            lConn.connect()

            val cleaner = new HtmlCleaner()
            val input = lConn.getInputStream()
            val rootNode = cleaner.clean(input)

            val eleCol = rootNode.getElementsByAttValue("id", id, true, true)
            if (eleCol != null && eleCol.length > 0){           // sometime the tag is not there (like Amazon's price tag).
                result = eleCol(0).getText.toString.trim        // get text from children also
            }
        }
       catch{
           // potentially many errors: such as connection refused (invalid web address...)
           case e: Exception => result = "(Error: %s)".format(e.getMessage)
       }

        // return the complete string
        return url + "|" + id + "|" + result
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

                val pool: ExecutorService = Executors.newFixedThreadPool(3)
                val handleCol: MutableList[Future[String]] = MutableList()

                try{
                    println("***111***")
                    for (host <- hosts){
                        val arr = host.split("\\|")
                        val handle: Future[String] = pool.submit(new ExtractTextFromHtml(arr(0), arr(1)))
                        handleCol += handle

                        //val etfh = new ExtractTextFromHtml(arr(0), arr(1))  // "priceblock_ourprice"
                        //store(host + "|" + etfh.extract())                  // a single string
                        //Thread.sleep(500)                                   // no need to hurry
                    }

                    // reject new tasks, but execute all already submitted tasks.
                    pool.shutdown()

                    println("***222***")
                    // Now wait everyone and retrieve the results
                    for(handle <- handleCol){
                        val piece: String = handle.get()    // wait until this task is finished, and then get the result
                        store(piece)                        // save to RDD
                    }
                    println("***333***")
                    // very unlikely to wait here...
                    pool.awaitTermination(10, TimeUnit.SECONDS)
                }
                catch {
                    // if awaitTermination() timeouts
                    case i: InterruptedException => Unit    // do nothing!
                }
                finally {
                    // Force thread-pool to shutdown
                    if (!pool.isTerminated)
                        pool.shutdownNow()
                }

                Thread.sleep(3000)      // otherwise, if the list of hosts is short, is this kind of DDOS attack?
            }// end while

            // No exception so far! Schedule to restart
            restart("Trying to connect again")
        }
        catch {
            // for other kinds of problems
            case e: Exception => restart("Error receiving data", e)
        }
    }
}

/*
Reference:
http://alvinalexander.com/scala/scala-html-parsing
https://github.com/cecol/testSample/blob/master/src/main/scala/test/app/xpath/Xpath.scala
https://twitter.github.io/scala_school/concurrency.html
https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Future.html
https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/Executor.html
 */
