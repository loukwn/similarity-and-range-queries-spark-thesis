# Similarity and range queries in Spark

This is the code that accompanies my diploma thesis "Processing of similarity and range queries in cloud systems". It was developed in Scala and it uses the Spark API so that it can be run in a distributed environment.

## Overview

The goal of this thesis is to answer complex queries of the form:

> Given a collection of documents return the pairs that contain the term A ```[minA,maxA]``` times, the term B ```[minB,maxB]``` times.. etc. From these pairs filter out the ones that have less than ```x%``` similarity between their docs.

The code uses the [MinHash](https://en.wikipedia.org/wiki/MinHash "MinHash - Wikipedia") technique to reduce the time it takes to calculate a [Jaccard](https://en.wikipedia.org/wiki/Jaccard_index "Jaccard Index - Wikipedia") similarity between two documents, as well as the technique of [Locality Sensitive Hashing](https://en.wikipedia.org/wiki/Locality-sensitive_hashing "Locality Sensitive Hashing - Wikipedia") to reduce the number of times that MinHash will be run during the execution of the aforementioned complex query.

## Screenshots

Comparison of the different methods, running on the platform Databricks (Community version) and using [20Newsgroups](http://qwone.com/~jason/20Newsgroups/ "20Newsgroups") as the input dataset (notebook is included in the repo):

Execution of Minhash (signature size: 200) ->  **Time: 12.39 sec**

![](/screens/minhash.png "Execution of Minhash (signature size: 200)")

Execution of Minhash + LSH (signature size: 200, bands: 25, rows: 8) -> **Time: 9.01 sec**

![](/screens/lsh.png "Execution of Minhash + LSH (signature size: 200, bands: 25, rows: 8)")

Execution of plain Jaccard -> **Time: 31.52 sec**

![](/screens/jaccard.png "Execution of plain Jaccard")

## License
[MIT License](LICENSE)
