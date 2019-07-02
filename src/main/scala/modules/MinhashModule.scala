package modules

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random.nextInt

/*
 *  MinhashModule: It generates the shingles, combining lists of tokens of size: wordsPerShingle and
 *  then generates the signature that will be used to represent the document in the similarity queries
*/

class MinhashModule extends BaseModule {

    private def generateShingles(docDF: DataFrame): DataFrame = {

        val genShingles = udf((tokens: Seq[String], wordsPerShingle: Int) => {
            var shinglesOfDocSet = mutable.Set[Int]()

            if (wordsPerShingle > tokens.length) {
                val shingle = tokens.mkString(" ")
                shinglesOfDocSet += shingle.## & 0xffffffff
            } else {
                for (i <- 0 to tokens.length - wordsPerShingle) {
                    // concatenate the tokens into a single space-separated shingle
                    val shingle = tokens.slice(i, i + wordsPerShingle).mkString(" ")
                    shinglesOfDocSet += shingle.## & 0xffffffff
                }
            }

            shinglesOfDocSet.toArray
        })

        docDF.withColumn("shingles", genShingles(col("tokens"), lit(execConfig.wordsPerShingle)))
    }

    private def calculateSignatures(a: List[Int], b: List[Int], docDF: DataFrame): DataFrame = {


        val calcSignatures = udf((shingles: Seq[Long], numOfHashFunctions: Int, a: Seq[Int], b: Seq[Int]) => {
            // we use the a, b random lists to implement the hash functions and execute the MinHash algo

            // this is the first prime after the biggest 32-bit hash that can be generated as a shingle (to avoid collisions)
            val p = 4294967311L

            var signature = new ListBuffer[Long]()

            // generate the hashes and keep the minimum for each permutation (minhash)
            for (i <- 0 until numOfHashFunctions) {
                var minHash = p + 1

                shingles.foreach(shingleId => {
                    val hash = (a(i) * shingleId + b(i)) % p

                    if (hash < minHash) minHash = hash
                })

                signature += minHash
            }

            signature.toArray
        })

        docDF.withColumn("signature", calcSignatures(col("shingles"), lit(execConfig.numOfHashFunctions), typedLit(a), typedLit(b))).drop("shingles")
    }

    private def generateCoeffsForHashFunctions(): List[Int] = {
        // Our hash functions will be in the form: h = (a*x + b) mod p (p is explained in calculateSignatures()).
        // Here we will generate the a, b coefficients and return them.
        // (We need to ensure that each random number is unique)
        val maxId = scala.math.pow(2, 32).toInt - 1

        // loop to generate the nums using the set to ensure unique nums
        val rands = mutable.HashSet[Int]()
        while (rands.size < execConfig.numOfHashFunctions) {
            rands += nextInt(maxId)
        }

        // return the list
        rands.toList
    }

    // public methods/functions

    def minHash(docDF: DataFrame): DataFrame = {
        hasBeenExecuted = true

        // because we need to mutate the dataframe
        var newDF = docDF

        // the coefficients a,b for the hash functions  (explained in comment inside the function)
        val a = generateCoeffsForHashFunctions()
        val b = generateCoeffsForHashFunctions()

        // execute minhash algorithm
        newDF = generateShingles(newDF)
        newDF = calculateSignatures(a, b, newDF)

        // return the updated dataframe
        newDF
    }
}
