import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Main {
  
  // Extract (ent, name, #(e,n)) from one line of form <url><tab><cprob><space><string>[<tab><score>[<space><score>]*]
  def parseOneLineAndExtractMentionAndCounters(ent: String, linesIterator: Iterable[String]) : Array[(String, Int)] = {
    var mentionCountsArray = Array.empty[(String, Int)]
    for (line <- linesIterator) {
      val terms = line.split("\t")
      if (terms.size != 3) {
        throw new Exception("Crosswikis line is not well formated: " + line)
      }
      
      val name = terms(1).substring(terms(1).indexOf(' ') + 1)
      
      var numNameEnt = 0
      for (serialScore <- terms(2).split(" ")) {
        val reducedSerialScore = serialScore.split(":")
        if (reducedSerialScore.head != "Wx") {
          numNameEnt += reducedSerialScore.last.split("/").head.toInt
        }
      }
      if (numNameEnt >= 5) {
    	  mentionCountsArray :+= (name, numNameEnt)
      }
    }
    mentionCountsArray 
  }
  
  // Human readable output formatting.
  def toString_fullNamesMapForOneEntity(ent: String, namesMap : Array[(String, Int)]) = {
    var output = ent + "\t==>\n" 
    for ((name, counter) <- namesMap) {
      output += "\t" + name + "\t" + counter + "\n"
    }
    output
  }
  
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("Crosswikis cleaning")
    val sc = new SparkContext(conf)
    val logFile = args(0);
    
    val dataInRDD = sc.textFile(logFile).map(line => { val ent = line.split("\t").head; (ent , line)} )
    									.groupByKey
    									.map{ case (ent, linesIter) => (ent, parseOneLineAndExtractMentionAndCounters(ent, linesIter)) }
    
    
    
    // Print the output data: 
    dataInRDD.sortByKey(true)
    		.map{ case (ent, namesMap) => toString_fullNamesMapForOneEntity(ent, namesMap) }
    		.saveAsTextFile(args(1))
  }
  
}
