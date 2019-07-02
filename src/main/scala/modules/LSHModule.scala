package modules

import org.apache.spark.sql.functions.{col, desc, lit, typedLit, udf}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/*
 * LSHModule: It provides an extra measure to speed up the Minhash algorithm.
 *
 * It splits the signature of each doc into bands (of length: rows). The bands are then placed in buckets,
 * so that a signature will belong to many buckets.
 *
 * During a similarity query, only the signatures that have at least one bucket in common will be checked for similarity,
 * thus reducing the number of checks to be performed.
*/

class LSHModule extends BaseModule {

    def generateBuckets(docDF: DataFrame): DataFrame = {
        hasBeenExecuted = true

        val genBuckets = udf((signature: Seq[Long], rows: Int, bands: Int) => {

            def calculateBandHash(sign: List[Long], bandNum: Int): Long = {

                val startIndex = bandNum * rows
                val sb = mutable.StringBuilder.newBuilder

                // append all the rows of the band to a stringbuilder
                for (i <- startIndex until startIndex + rows) {
                    sb.append(sign(i))
                }

                // make it a string and output its hash
                sb.toString.##
            }


            // loop bands times and output the buckets that the signature (or the doc) will be placed in
            var bucketsForDoc = List[Long]()
            (0 until bands).foreach(i => {
                val key = calculateBandHash(signature.toList, i)
                bucketsForDoc = bucketsForDoc :+ key
            })

            // return the list of buckets that will contain a part of the signature
            bucketsForDoc.toArray

        })

        docDF.withColumn("buckets", genBuckets(col("signature"), lit(execConfig.numOfRows), lit(execConfig.numOfBands)))
    }
}
