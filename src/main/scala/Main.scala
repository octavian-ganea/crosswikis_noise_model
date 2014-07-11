import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap

object Main {
  
  // Remove names containing "Wikipedia", "wikipedia" or numbers ("[1]" , "[3]", etc)
  def isGeneralPlaceholder(ent : String, name : String) : Boolean = {
    if ((name.contains("Wikipedia") || name.contains("wikipedia")) && !ent.contains("Wikipedia") && !ent.contains("wikipedia")) 
      true
    else if (name.filter(!Character.isDigit(_)) == "[]")
      true
    else
      false
  }
  
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
      if (numNameEnt >= 5 && !isGeneralPlaceholder(ent, name) ) {
    	  mentionCountsArray :+= (name, numNameEnt)
      }
    }
    // Sort array descending by the counts of the name
    mentionCountsArray.sortWith( (x, y) => x._2 > y._2)
  }
  
   // Human readable output formatting.
  def toString_fullNamesMapForOneEntity(ent: String, namesMap : Array[(String, Int)]) : String = {
    var output = ent + "\t==>\n" 
    for ((name, counter) <- namesMap) {
      output += "\t" + name + "\t" + counter + "\n"
    }
    if (namesMap.size == 0) ""
    else output
  }
  
  def computeInitialThetas(ent: String, namesMap : Array[(String, Int)]) : HashMap[String, (Int, Double)] = {
    var total_num_e = 0.0 
	for ((name, counter) <- namesMap) {
	  total_num_e += counter
	}
    var mentionThetasArray : HashMap[String, (Int, Double)] = HashMap()
	for ((name, counter) <- namesMap) {
	  mentionThetasArray += (name -> (counter, counter / total_num_e))
	}    
    mentionThetasArray
  }
  
  def p_x_cond_m_beta(x : String, m : String) : Double = {
    var rez = 1.0
    for (i <- 0 to Math.min(x.size,m.size) - 1) {
      if (x(i) == m(i))
        rez *= 0.5
      else if (x(i) >= 32 && x(i) < 128)
        rez *= 0.5/97
      else // Don't support other chars except from [32,127] for the moment.
        rez = 0
    }
    
    for (i <- Math.min(x.size,m.size) to Math.max(x.size,m.size) - 1) {
      rez *= 0.5/97
    }
    rez
  }

  def computeSecondThetas(ent: String, namesFirstThetasMap : HashMap[String, (Int, Double)]) : HashMap[String, (Double, Double)] = {
    var numitorsMap : HashMap[String, Double] = HashMap()
    for ((n, (num_n_e, theta_n_0)) <- namesFirstThetasMap) {
      var numitor_n = 0.0
	  for ((m_prim, (num_m_prim_e, theta_m_prim_0)) <- namesFirstThetasMap) {
	    numitor_n += theta_m_prim_0 * p_x_cond_m_beta(n, m_prim)
	  }
      numitorsMap += (n -> numitor_n)
    }

    var mentionThetasArray : HashMap[String, (Double, Double)] = HashMap()
    var Z_thetas_1 = 0.0
    for ((m, (num_m_e, theta_m_0)) <- namesFirstThetasMap) {
	  var theta_m_1 = 0.0
	  for ((n, (num_n_e, theta_n_0)) <- namesFirstThetasMap) {
	    theta_m_1 += num_n_e * theta_m_0 * p_x_cond_m_beta(n, m) / numitorsMap(n)
	  }
	  Z_thetas_1 += theta_m_1
	  mentionThetasArray += (m -> (theta_m_0, theta_m_1))
	}
    
    for ((m, (theta_m_0, theta_m_1)) <- mentionThetasArray) {
      mentionThetasArray(m) =  (theta_m_0, theta_m_1 / Z_thetas_1)
    }
    mentionThetasArray
  }

  def computeUncorruptedNames(ent: String, namesAllThetasMap : HashMap[String, (Double, Double)]) : HashMap[String, (String, Double, Double)] = {
    var mentionThetasArray : HashMap[String, (String, Double, Double)] = HashMap()
    
    for ((n, (theta_n_0, theta_n_1)) <- namesAllThetasMap) {
      var real_n = n
      var score_real_n = 0.0
      for ((m, (theta_m_0, theta_m_1)) <- namesAllThetasMap) {
        if (theta_m_1 * p_x_cond_m_beta(n, m) > score_real_n) {
          real_n = m
          score_real_n = theta_m_1 * p_x_cond_m_beta(n, m)
        }
      }
      mentionThetasArray += (n -> (real_n, theta_n_0, theta_n_1))
    }
    mentionThetasArray
  }
  
  
  // Human readable output formatting.
  def toString_ThetasMapForOneEntity(ent: String, namesMap : HashMap[String, (String, Double, Double)]) : String = {
    var output = ent + "\t==>\n" 
    for ((name, (real_n, theta_0, theta_1)) <- namesMap) {
      output += "\t" + name + "\tReal=" + real_n + "\t" + theta_1 + "\t(" + theta_0 + ")\n"
    }
    if (namesMap.size == 0) ""
    else output
  }
  
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("Crosswikis cleaning")
    val sc = new SparkContext(conf)
    val logFile = args(0);
    
    val dataInRDD = sc.textFile(logFile).map(line => { val ent = line.split("\t").head; (ent , line)} )
    									.groupByKey
    									.map{ case (ent, linesIter) => (ent, parseOneLineAndExtractMentionAndCounters(ent, linesIter)) }
    
    val thetasRDD = dataInRDD.map{ case (ent, namesMap) => (ent, computeInitialThetas(ent, namesMap)) }
    						 .map{ case (ent, namesTheta0Map) => (ent, computeSecondThetas(ent, namesTheta0Map))}
    						 .map{ case (ent, namesTheta0Map) => (ent, computeUncorruptedNames(ent, namesTheta0Map))}    						 
    						 .map{ case (ent, namesTheta0Map) => toString_ThetasMapForOneEntity(ent, namesTheta0Map) }
    						 .filter{ text => text != "" }
    						 .saveAsTextFile(args(1))
    
    /*
    // Print the output data: 
    dataInRDD.sortByKey(true)
			.map{ case (ent, namesMap) => toString_fullNamesMapForOneEntity(ent, namesMap) }
    		.filter{ text => text != "" }
			.saveAsTextFile(args(1))
     */
  }
  
}
