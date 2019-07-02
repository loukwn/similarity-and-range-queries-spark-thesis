package extras

// This class is used to pass the term preferences when making a term query

case class TermData(term: String, minTimes: Int, maxTimes: Int)
