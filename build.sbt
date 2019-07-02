name := "SparkWorkspace"

version := "0.1"

scalaVersion := "2.11.9"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.0", 
                            "org.apache.spark" %% "spark-sql" % "2.4.0",
                            "org.apache.spark" %% "spark-mllib" % "2.4.0",
                            
                            "mrpowers" % "spark-daria" % "0.26.1-s_2.12")