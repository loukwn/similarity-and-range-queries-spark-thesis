package modules

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, StringType}

import scala.io.Source

/*
 * This module handles the io between the disk and spark. Also is responsible for error printing and console output
 * when it is necessary
*/

class IOModule extends BaseModule {

    // Console io
    def printInfo(mess: String, addNewLine: Boolean = true): Unit = {
        if (execConfig.verbose) print(mess + (if (addNewLine) "\n" else ""))
    }

    def printError(mess: String, addNewLine: Boolean = true): Unit = {
        sys.error(mess + (if (addNewLine) "\n" else ""))
    }

    // Disk io
    def readDataFromSource(): DataFrame = {
        var trainList = List[String]()

        val input = Source.fromFile(execConfig.inputFilePath)
        trainList = input.getLines().toList
        input.close()

        val docData = preprocessStrings(trainList)

        // the final input dataframe
        sparkSession.createDF(
            docData
            , List(
                // schema
                ("id", StringType, false),
                ("tokens", ArrayType(StringType, containsNull = false), true)
            )
        )
    }
    
    // this function loops the docs, filters those that have less than wordsPerShingle tokens and stores their index too
    private def preprocessStrings(listOfDocsAsStrings: List[String]): List[(String, Array[String])] = {
        var docList: List[(String, Array[String])] = List()

        // will remove empty docs and those that have a length less than the wordsPerShingle
        listOfDocsAsStrings.map(_.split("\\s").toList.filterNot(_ == "").map(_.toLowerCase())).filter(_.size >= execConfig.wordsPerShingle).zipWithIndex.foreach { case (tokens, index) =>
            docList = docList :+ ((index.toString, tokens.drop(1).toArray))
        }

        docList
    }
}
