package modules

import extras.{ExecConfigBuilder, ExecConfig}
import org.apache.spark.sql.SparkSession

// Here lie all the common fields and methods/functions for every submodule, so that the boilerplate code will be reduced

abstract class BaseModule {

    var execConfig: ExecConfig = new ExecConfigBuilder().build
    var sparkSession: SparkSession = _
    var hasBeenExecuted: Boolean = false

    // Setters and getters
    def setExecConfig(execConfig: ExecConfig): Unit = {
        this.execConfig = execConfig
    }

    def setSparkSession(sparkSession: SparkSession): Unit = {
        this.sparkSession = sparkSession
    }

    // extras
    def checkExecuted: Boolean = hasBeenExecuted
}
