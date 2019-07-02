package modules

import extras.TermData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, typedLit, udf}

import scala.collection.mutable.ListBuffer

/*
 * MinMaxModule: it is responsible for filtering the dataframe during a query.
 * It basically keeps the ones that contain the terms provided by the user in the range of
 * [min, max] times and (temporarily) discards the others
*/

class MinMaxModule extends BaseModule {

    def filterByMinMax(docDF: DataFrame, termList: List[TermData]): DataFrame = {

        // udf that calculates the given term occurences
        val calcOccurencesAndFilter = udf((tokens: Seq[String], terms: Seq[String], reqMinOccurences: Seq[Int], reqMaxOccurences: Seq[Int]) => {

            // the list for the occurence of each term
            val occurences = Array.fill(terms.length)(0)
            var retValue = true

            // then we loop for every token and update the previous list
            tokens.foreach(token => {
                terms.indices.foreach(i => {
                    if (token.equals(terms(i))) occurences(i) += 1
                })
            })

            // then we filter
            occurences.zipWithIndex.foreach{case (value, index) =>
                if (value < reqMinOccurences(index) || value > reqMaxOccurences(index)) retValue = false
            }

            // a simple boolean is returned
            retValue
        })

        // parse input
        val termsBuffer = new ListBuffer[String]()
        val reqMinOccurencesBuffer = new ListBuffer[Int]()
        val reqMaxOccurencesBuffer = new ListBuffer[Int]()

        termList.foreach(termData => {
            termsBuffer += termData.term
            reqMinOccurencesBuffer += termData.minTimes
            reqMaxOccurencesBuffer += termData.maxTimes
        })


        // apply the udf
        val tempDocDF = docDF.withColumn("filterValue",
            calcOccurencesAndFilter(
                col("tokens"),
                typedLit(termsBuffer.toList),
                typedLit(reqMinOccurencesBuffer.toList),
                typedLit(reqMaxOccurencesBuffer.toList)))

        // return the filtered dataframe
        tempDocDF.filter(tempDocDF("filterValue")).drop("filterValue")
    }
}
