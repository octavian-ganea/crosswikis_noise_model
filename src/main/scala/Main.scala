import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.broadcast.Broadcast
import Array._
import ParseOriginalCrosswikisFile.parseOneLineAndExtractMentionAndCounters
import Betas.p_y_cond_x
import org.apache.spark.storage.StorageLevel


object Main {
  
  // Controls the percentage of probabilities that we allow for noisy strings : \sum_m p(m) * p(m|m;beta) = SPARSNESS_CT
  val SPARSNESS_CT = 0.7  
   
  // Computes uncorrupted(n) = argmax_m theta_m * p(n|m;beta)
  // Input: hashmap {m : (#(m,e), theta0_m, theta1_m)}
  // Output: hashmap {m : (uncorrupted(m), #(m,e), theta1_m)}
  def computeUncorruptedRepresentativeNames(
      namesAllThetasMap : Array[(String, Int, Double)], c : Broadcast[CondTransducer]) : Array[(String, String, Int, Double)] = {
    
    val thetasArray = new Array[(String, String, Int, Double)](namesAllThetasMap.size)
    
    var index = 0
    for ((n, num_n_e, theta_n) <- namesAllThetasMap) {
      var real_n = n
      var score_real_n = 0.0
      for ((m, num_m_e, theta_m) <- namesAllThetasMap) {
        val p_n_cond_m = p_y_cond_x(n, m, c.value)
        if (theta_m * p_n_cond_m > score_real_n) {
          real_n = m
          score_real_n = theta_m * p_n_cond_m
        }
      }
      thetasArray(index) = (n, real_n, num_n_e, theta_n)
      index = index + 1
    }
    thetasArray
  }
  
 
  def main(args: Array[String]) : Unit = {    
    UnitTests.tests
    
    if (args.length < 2) {
      System.err.println("Usage: input_file output_file")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Crosswikis cleaning")
    conf.set("spark.cores.max", "128")
    
    conf.set("spark.akka.frameSize", "1024")
    conf.set("spark.executor.memory", "100g")
    conf.set("spark.shuffle.file.buffer.kb", "1000")
    conf.set("spark.broadcast.compress", "true")

    
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "MyRegistrator")
    conf.set("spark.kryoserializer.buffer.mb", "40")
    
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")
    
    val sc = new SparkContext(conf)
    
    val condTransducer = new CondTransducer
    condTransducer.initC_0
    var c = sc.broadcast(condTransducer)
    
    val entNamesRDDMap = sc.textFile(args(0))
    	.map(line => { val ent = line.split("\t").head; (ent , line)} ) // Extract entity first
    	.groupByKey  // Group by entity
    	.map{ case (ent, linesIter) => (ent, parseOneLineAndExtractMentionAndCounters(ent, linesIter)) }
    	.filter{ case (ent, namesMap) => namesMap.size > 0 } // we removed all n with #(n,e) <= 5

    val thetas = entNamesRDDMap
    	.mapValues{ namesMap => Thetas.computeInitialThetasForOneEnt(namesMap) }
    	.mapValues{ namesTheta0Map => Thetas.updateThetasForOneEnt(namesTheta0Map, c)}
    	.mapValues{ namesTheta1Map => computeUncorruptedRepresentativeNames(namesTheta1Map, c)}
    	.persist(StorageLevel.MEMORY_AND_DISK )
    	
    val deltas = thetas
    	.map{ case (ent, namesThetaUncorrupted) => Betas.computeDeltas(namesThetaUncorrupted, c) }
    	// The reduceByKey operation performs partial aggregations on the mapper nodes, like MapReduce’s combiners.
    	.reduce((mat1, mat2) => Utils.addMatrices(mat1, mat2))
    	

    println ("Results : " )    
    for (i <- 0 to 96) {
      for (j <- 0 to 96) {
        print(deltas(i)(j) + " ")
      }
      println()
    }	
    	
    	/*
    // Output results
    thetas.map{ case (ent, namesMap) => Utils.toString_ThetasMapForOneEntity(ent, namesMap) }
    	.saveAsTextFile(args(1))
    	*/
    sc.stop()
  }
  
}
