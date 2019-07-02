import extras.{ExecConfigBuilder, TermData}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object Main {

    def main(args: Array[String]): Unit = {

        // spark session object
        print("Initializing Spark...")
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("MinHashLSHSparked")
          .getOrCreate()
        println("OK")

        // algorithm execution parameters
        val execConfig = new ExecConfigBuilder()
          .inputFilePath("src/main/resources/docs.txt")
          .numOfHashFunctions(100)
          .numOfBands(10)
          .numOfRows(10)
          .wordsPerShingle(1)
          .runAlsoLSH(false)
          .build

        // initialize the interface
        print("Initializing interface...")
        val interfaceObj = new SparkInterface(spark, execConfig)
        println("OK")

        // test execution
        var run = true
        while (run) {
            print("\nWhat to do?\n1) Give id for search\n2) Query term\n3) Exit\n\n> ")
            val sel = scala.io.StdIn.readLine().toInt

            if (sel == 1) {
                print("Enter doc id: ")
                val id = scala.io.StdIn.readLine()

                print("Enter desired similarity: ")
                val similarity = scala.io.StdIn.readFloat()

                time(interfaceObj.checkForSimilarDocs(id, similarity))
            } else if (sel == 2) {

                val termListBuffer = new ListBuffer[TermData]()
                var run = true

                while (run) {
                    print("Enter search term or just hit [enter] to stop: ")
                    val term = scala.io.StdIn.readLine()
                    if (term.length==0) {
                        run = false
                    } else {
                        print("Enter min occurences of the term: ")
                        val minOcc = scala.io.StdIn.readInt()

                        print("Enter max occurences of the term: ")
                        val maxOcc = scala.io.StdIn.readInt()

                        termListBuffer += TermData(term, minOcc, maxOcc)
                    }
                }
                print("\nEnter desired similarity: ")
                val similarity = scala.io.StdIn.readFloat()

                // run my implementation
                time(interfaceObj.queryTerms(termListBuffer.toList, similarity))

                // run naive jaccard
                time(interfaceObj.queryTerms(termListBuffer.toList, similarity, goNaive = true))
            } else {
                run = false
            }
        }

        // exit gracefully
        println("Bye!")
        spark.stop()
    }

    // utility function to time the function calls
    def time[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block    // call-by-name
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
        result
    }
}
