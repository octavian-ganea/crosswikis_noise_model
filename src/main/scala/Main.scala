import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import ParseOriginalCrosswikisFile.parseOneLineAndExtractMentionAndCounters
import Betas.p_x_cond_m_beta


object Main {
  
  // Controls the percentage of probabilities that we allow for noisy strings : \sum_m p(m) * p(m|m;beta) = SPARSNESS_CT
  val SPARSNESS_CT = 0.7  
   
  // Computes uncorrupted(n) = argmax_m theta_m * p(n|m;beta)
  // Input: hashmap {m : (#(m,e), theta0_m, theta1_m)}
  // Output: hashmap {m : (uncorrupted(m), #(m,e), theta0_m, theta1_m)}
  def computeUncorruptedRepresentativeNames(namesAllThetasMap : HashMap[String, (Int, Double)]) : HashMap[String, (String, Int, Double)] = {
    val thetasArray : HashMap[String, (String, Int, Double)] = HashMap()
    
    for ((n, (num_n_e, theta_n_1)) <- namesAllThetasMap) {
      var real_n = n
      var score_real_n = 0.0
      for ((m, (num_m_e, theta_m_1)) <- namesAllThetasMap) {
        if (theta_m_1 * p_x_cond_m_beta(n, m) > score_real_n) {
          real_n = m
          score_real_n = theta_m_1 * p_x_cond_m_beta(n, m)
        }
      }
      thetasArray += (n -> (real_n, num_n_e, theta_n_1))
    }
    thetasArray
  }
  
 
  def main(args: Array[String]) : Unit = {
	if (args.length < 2) {
      System.err.println("Usage: input_file output_file")
      System.exit(1)
    }
	
    val conf = new SparkConf().setAppName("Crosswikis cleaning")
    val sc = new SparkContext(conf)
    
    val entNamesRDDMap = sc.textFile(args(0))
    						.map(line => { val ent = line.split("\t").head; (ent , line)} ) // Extract entity first
    						.groupByKey  // Group by entity
    						.map{ case (ent, linesIter) => (ent, parseOneLineAndExtractMentionAndCounters(ent, linesIter)) }
    						.filter{ case (ent, namesMap) => namesMap.size > 0 } // we removed all n with #(n,e) <= 5

    val thetasRDD = entNamesRDDMap
    						.mapValues{ namesMap => Thetas.computeInitialThetasForOneEnt(namesMap) }
    						.mapValues{ namesTheta0Map => Thetas.updateThetasForOneEnt(namesTheta0Map)}
    						.mapValues{ namesTheta1Map => computeUncorruptedRepresentativeNames(namesTheta1Map)}
    						.map{ case (ent, namesMap) => Utils.toString_ThetasMapForOneEntity(ent, namesMap) }
    						.saveAsTextFile(args(1))
  }
  
}
