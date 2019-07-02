package modules

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, DataFrame}

/*
 *  JaccardModule: performs the naive Jaccard method, just so we can compare it with our own
*/

class JaccardModule extends BaseModule {

    def performJaccardSimilarity(df: DataFrame, id1Col: Column, tok1Col: Column, id2Col: Column, tok2Col: Column): DataFrame = {

        // udf that generates the similarity between the signatures of the doc we want against all the others
        val genJaccardSimilarity = udf((id: String, tokens: Seq[String], idToSearch: String, tokensToSearch: Seq[String]) => {
            var res : Double = 0.0f

            if (!id.equals(idToSearch)) {
                val seta = tokens.toSet
                val setb = tokensToSearch.toSet

                res = seta.intersect(setb).size.toFloat / seta.union(setb).size
            }

            res
        })

        df.withColumn("similarity", genJaccardSimilarity(id1Col, tok1Col, id2Col, tok2Col))
    }
}
