package extras

/*
* This class stores the user defined execution parameters of the minhash LSH algorithm, so that it can be passed to
* every module that might need them
*/

class ExecConfig (configBuilder: ExecConfigBuilder) {

    var inputFilePath: String = configBuilder.inputFilePath
    var wordsPerShingle: Int = configBuilder.wordsPerShingle
    var numOfHashFunctions: Int = configBuilder.numOfHashFunctions
    var numOfBands: Int = configBuilder.numOfBands
    var numOfRows: Int = configBuilder.numOfRows
    var runAlsoLSH: Boolean = configBuilder.runAlsoLSH
    var verbose: Boolean = configBuilder.verbose
}

class ExecConfigBuilder {
    var inputFilePath: String = "src/main/resources/docs.txt"
    var wordsPerShingle: Int = 1
    var numOfHashFunctions: Int = 200
    var numOfBands: Int = 50
    var numOfRows: Int = 4
    var runAlsoLSH: Boolean = true
    var verbose: Boolean = true

    def inputFilePath(filePath: String): ExecConfigBuilder = {
        this.inputFilePath = filePath
        this
    }

    def wordsPerShingle(wordsPerShingle: Int): ExecConfigBuilder = {
        this.wordsPerShingle = wordsPerShingle
        this
    }

    def numOfHashFunctions(numOfHashFunctions: Int): ExecConfigBuilder = {
        this.numOfHashFunctions = numOfHashFunctions
        this
    }

    def numOfBands(numOfBands: Int): ExecConfigBuilder = {
        this.numOfBands = numOfBands
        this
    }

    def numOfRows(numOfRows: Int): ExecConfigBuilder = {
        this.numOfRows = numOfRows
        this
    }

    def runAlsoLSH(runAlsoLSH: Boolean): ExecConfigBuilder = {
        this.runAlsoLSH = runAlsoLSH
        this
    }

    def verbose(verbose: Boolean): ExecConfigBuilder = {
        this.verbose = verbose
        this
    }

    def build: ExecConfig = new ExecConfig(this)
}
