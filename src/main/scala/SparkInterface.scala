import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import extras.{ExecConfig, ExecConfigBuilder, TermData}
import modules.{IOModule, JaccardModule, LSHModule, MinMaxModule, MinhashModule}
import org.apache.spark.sql.functions.{col, desc, lit, typedLit, udf}

import scala.collection.mutable

/*
 * This is the text similarity spark interface. It needs a SparkSession as an input, as well as an execConfig object,
 *  which will contain the execution parameters of the algorithm
 *
 *  The functionality of this class is bundled in 5 modules:
 *  1) ioModule:       contains the methods that are required for the disk and the console io (info/error)
 *  2) minHashModule:  is responsible for the generation of the shingles and the signatures for every text
 *  3) lshModule:      is responsible for the generation of the buckets
 *  4) minMaxModule:   is responsible for executing the minmax part of the term query
 *  5) jaccardModule:  is responsible for executing the naive jaccard method on the sets of tokens
 *
 *  This was done so that the code will respect the separation of concerns and to be able to test it easily
*/

class SparkInterface(sparkSession: SparkSession = null,
                     execConfig: ExecConfig = new ExecConfigBuilder().build,
                     ioModule: IOModule = new IOModule,
                     minHashModule: MinhashModule = new MinhashModule,
                     lshModule: LSHModule = new LSHModule,
                     minMaxModule: MinMaxModule = new MinMaxModule,
                     jaccardModule: JaccardModule = new JaccardModule) {

    // the dataframe of the input data
    var docDF: DataFrame = _

    // argument error checking
    if (sparkSession==null) this.ioModule.printError("Non null SparkSession object is required")

    // setup modules
    ioModule.setExecConfig(execConfig)
    ioModule.setSparkSession(sparkSession)
    minHashModule.setExecConfig(execConfig)
    minHashModule.setSparkSession(sparkSession)
    lshModule.setExecConfig(execConfig)
    lshModule.setSparkSession(sparkSession)
    minMaxModule.setExecConfig(execConfig)
    minMaxModule.setSparkSession(sparkSession)
    jaccardModule.setExecConfig(execConfig)
    jaccardModule.setSparkSession(sparkSession)

    //------- read input data

    // error checking
    if (execConfig.inputFilePath==null) this.ioModule.printError("Input file path was not provided")

    // read data from disk
    this.docDF = ioModule.readDataFromSource()


    //------- minhash

    // error checking
    if (this.docDF == null) this.ioModule.printError("Doc dataframe is empty. Cannot perform minhash")

    // perform minhash and return the results
    this.docDF = minHashModule.minHash(this.docDF)


    //------- lsh
    if (execConfig.runAlsoLSH) {
        // generate the buckets
        this.docDF = lshModule.generateBuckets(docDF)
    }

    // at this point the dataframe will no longer change, so we cache it for faster computations and queries
    this.docDF.cache()


    def checkForSimilarDocs(documentID: String, similarityThreshold: Float): Unit = {

        if (!minHashModule.hasBeenExecuted) {
            ioModule.printError("At least minhash needs to be run so that any similarity check can be performed")
        }

        // query for the doc that we are interested in
        val s = docDF.filter(docDF("id") === documentID).collect()

        if (s.isEmpty) {
            // the id is not present
            ioModule.printInfo("ID provided is not present in the document collection\n")

        } else if (s.length > 1) {
            // multiple docs with same id
            ioModule.printInfo("Multiple documents with the same id were found.. Skipping this search\n")

        } else {
            // all good

            var signature = Seq[Long]()
            var buckets = Seq[Long]()
            var docID: String = null

            // get the data from the query results
            s.foreach {
                case Row(id: String, _: Seq[String], sign: Seq[Long]) =>
                    signature = sign
                    docID = id

                case Row(id: String, _: Seq[String], sign: Seq[Long], bucks: Seq[Long]) =>
                    signature = sign
                    buckets = bucks
                    docID = id
            }

            // perform the similarity query (minhash only or minhash/LSH)
            val res = performSimilarityQuery(docDF, col("id"), col("signature"), col("buckets"), lit(docID), typedLit(signature), typedLit(buckets))

            // filter the results based on their similarity and sort them by descending order
            res.filter(res("similarity")>=similarityThreshold)
              .sort(desc("similarity"))
              .drop("tokens", "shingles", "signature", "buckets")
              .withColumnRenamed("id", "id of doc")
              .show()
        }
    }

    def queryTerms(termList: List[TermData], similarityThreshold: Float = 0.01f, goNaive: Boolean = false): Unit = {

        if (similarityThreshold < 0.0f || similarityThreshold > 1.0f) ioModule.printError("Invalid arguments. Should be: similarityThreshold>=0 && similarityThreshold<=1")

        // udf that will calculate the hash of the ids in a set so that the duplicates will be eliminated
        val calcIdHash = udf((id: String, id2: String) => { Set(id, id2).## })

        // here the range part of the query is executed
        val res = minMaxModule.filterByMinMax(this.docDF, termList)

        // create a copy of res
        val res2 = res
          .withColumnRenamed("id", "id2")
          .withColumnRenamed("signature", "signature2")
          .withColumnRenamed("tokens", "tokens2")
          .withColumnRenamed("buckets", "buckets2")

        // the two dataframes are cross joined, duplicate pairs removed (calcIdHash function), so we calculate the similarity and
        // we keep the rows above the provided threshold, while sorting them in descending order

        var res3 = res.crossJoin(res2)
          .withColumn("hash", calcIdHash(col("id"), col("id2")))
          .dropDuplicates("hash")
          .drop("hash")

        if (goNaive) {
            // naive jaccard

            res3 = jaccardModule.performJaccardSimilarity(res3, col("id"), col("tokens"), col("id2"), col("tokens2"))
              .drop("signature", "signature2", "buckets", "buckets2", "tokens", "tokens2")
        } else {
            // minhash/LSH similarity

            res3 = performSimilarityQuery(res3, col("id"), col("signature"), col("buckets"), col("id2"), col("signature2"), col("buckets2"))
              .drop("signature", "signature2", "buckets", "buckets2", "tokens", "tokens2")
        }

        // filter by similarity threshold and pretty print
        res3.filter(res3("similarity") >= similarityThreshold)
          .sort(desc("similarity"))
          .withColumnRenamed("id", "id of doc#1")
          .withColumnRenamed("id2", "id of doc#2")
          .withColumnRenamed("similarity", "similarity between them")
          .show(100)
    }

    // performs the distributed Minhash/LSH similarity check
    private def performSimilarityQuery(df: DataFrame, id1Col: Column, sign1Col: Column, buckets1Col: Column, id2Col: Column, sign2Col: Column, buckets2Col: Column) : DataFrame = {

        // udf that generates the similarity between the signatures of the doc we want against all the others
        val genSimilarity = udf((id: String, signature: Seq[Long], idToSearch: String, signatureToSearch: Seq[Long]) => {
            var res : Double = 0.0f
            if (!id.equals(idToSearch))
                res = signature.toList.zip(signatureToSearch.toList).count(x => x._1 == x._2).toFloat / signature.size
            res
        })

        // udf that compares the two arrays and returns true if they have at least one common element
        val checkForCommonElements = udf { (a: mutable.WrappedArray[Long], b: mutable.WrappedArray[Long]) =>
            if (a.intersect(b).nonEmpty){ true } else { false }
        }

        // gather the results using minhash (and LSH if we want)
        var res: DataFrame = null

        if (lshModule.hasBeenExecuted)
            res = df.filter(checkForCommonElements(buckets1Col, buckets2Col)).withColumn("similarity", genSimilarity(id1Col, sign1Col, id2Col, sign2Col))
        else
            res = df.withColumn("similarity", genSimilarity(id1Col, sign1Col, id2Col, sign2Col))

        res
    }
}
