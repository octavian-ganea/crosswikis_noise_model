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
  
  // Epsilon used for stopping the iterative main loop when convergence is achieved.
  val CONV_EPS = 0.0001
  
  // Controls the percentage of probabilities that we allow for noisy strings : \sum_m p(m) * p(m|m;beta) = SPARSNESS_CT
  val SPARSNESS_CT = 0.7  
   
  // Computes uncorrupted(n) = argmax_m theta_m * p(n|m;beta)
  // Input: array {(m, #(m,e), theta0_m, theta1_m)}
  // Output: array {(m, uncorrupted(m), #(m,e), theta1_m)}
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
    
    var condTransducer = new CondTransducer
    condTransducer.initC_0
    
    val entNamesRDDMap = sc.textFile(args(0))
    	.map(line => { val ent = line.split("\t").head; (ent , line)} ) // Extract entity first
    	.groupByKey  // Group by entity
    	.map{ case (ent, linesIter) => (ent, parseOneLineAndExtractMentionAndCounters(ent, linesIter)) }
    	.filter{ case (ent, namesMap) => namesMap.size > 0 } // we removed all n with #(n,e) <= 5

    var thetas = entNamesRDDMap
    	.mapValues{ namesMap => Thetas.computeInitialThetasForOneEnt(namesMap) }
    
    // Convergence error. Stop the iterative loop when error < CONV_EPS
    var error = 1.0
    var iterationNum = 1
    
    while (error > CONV_EPS) {
      println("==========================================================================")
      println("================== Starting iteration " + iterationNum + " =================================")
      println("================== current error : " + error + " =================================")
      println("==========================================================================")
      
      val c = sc.broadcast(condTransducer)

      val thetasAux = thetas
      	.mapValues{ namesTheta0Map => Thetas.updateThetasForOneEnt(namesTheta0Map, c)}
      	.persist(StorageLevel.MEMORY_AND_DISK)
      	
      thetas.unpersist(true)
      thetas = thetasAux
      
      val nameRepresentativesAndThetas = thetas
      	.mapValues{ namesTheta0Map => Thetas.updateThetasForOneEnt(namesTheta0Map, c)}
      	.mapValues{ namesTheta1Map => computeUncorruptedRepresentativeNames(namesTheta1Map, c)}
      	.persist(StorageLevel.MEMORY_AND_DISK)
    
      val deltas = nameRepresentativesAndThetas
    	.map{ case (ent, namesThetaUncorrupted) => Betas.computeDeltas(namesThetaUncorrupted, c) }
      	.reduce((mat1, mat2) => Utils.addMatrices(mat1, mat2))
    
      /*	
      for (a <- 0 to 96) {
      	for (b <- 0 to 96) {
      	  var aa = "" + (a + 31).toChar
      	  var bb = "" + (b + 31).toChar
      	  if (a == 0) aa = "eps"
      	  if (b == 0) bb = "eps"
      	  println("delta(" + bb + "|" + aa + ") = " + deltas(a)(b))
      	}
      }
      */
    
      val unnormalizedFFs = nameRepresentativesAndThetas
      	.map{ case (ent, namesThetaUncorrupted) => Betas.computeFFs(namesThetaUncorrupted, c) }
      	.reduce((mat1, mat2) => Utils.addMatrices(mat1, mat2))
    	
      val S = deltas(0)(0)

      // Compute \sum_m num_m_e * alpha(m|m)
      val unnormalizedIdStringsMass = nameRepresentativesAndThetas
      	.map{ case (ent, namesThetaUncorrupted) => Betas.computeIdenticalStringsMass(namesThetaUncorrupted, c) }
      	.reduce(_ + _)
    
      // Compute \sum_m p(m) * alpha(m|m) by normalizing identicalStringsMass
      val idStringMass = unnormalizedIdStringsMass / S
      	
      // This should be identical with SPARSNESS_CT. Otherwise the sparsness condition is not satisfied, which is wrong.
      assert(Math.abs(SPARSNESS_CT - idStringMass * condTransducer.getFromIntIndexes(0,0)) < 0.0001, 
          " Identical string mass so far : " + idStringMass * condTransducer.getFromIntIndexes(0,0))
      
      nameRepresentativesAndThetas.unpersist(true)
      
      var Narray = Array.fill[Double](97)(0)
      var Farray = Array.fill[Double](97)(0)
      var F = SPARSNESS_CT
      var N = S
      
      for (a <- 0 to 96) {
        for (b <- 0 to 96) {
          if (a != 0 || b != 0) {
            Narray(a) += deltas(a)(b)
            Farray(a) += unnormalizedFFs(a)(b)
          }
        }
        // Normalize F's
        Farray(a) /= S
        F += Farray(a)
        N += Narray(a)
        println("F(" + a + ") = " + Farray(a))
        println("N(" + a + ") = " + Narray(a))
      }
      	
      println()
      val lambda = ((N - Narray(0)) * idStringMass - SPARSNESS_CT * N) / ((F - Farray(0)) * idStringMass - SPARSNESS_CT * F)
      val miu = ((N - lambda * F) / (N - Narray(0) -  lambda * (F - Farray(0)))) * (S - lambda * SPARSNESS_CT)
      
      var miuArray = Array.fill[Double](97)(0)
      for (a <- 1 to 96) {
        miuArray(a) = ((N - lambda * F) / (N - Narray(0) -  lambda * (F - Farray(0)))) * (Narray(a) - lambda * Farray(a))
        println("miu(" + a + ") = " + miuArray(a))
      }
    
      println ("lambda = " + lambda)
      println ("miu = " + miu)
      println ("N = " + N)
      println ("F = " + F)
    
      println("Final results : ")
    
      error = 0.0
      for (a <- 0 to 96) {
        for (b <- 0 to 96) {
          var aa = "" + (a + 31).toChar
          var bb = "" + (b + 31).toChar
          if (a == 0) aa = "eps"
          if (b == 0) bb = "eps"
          var v = 0.0
          if (a != 0) {
            v = (deltas(a)(b) - lambda * unnormalizedFFs(a)(b) / S) / miuArray(a)
          } else if (b != 0) {
            v = (deltas(0)(b) - lambda * unnormalizedFFs(0)(b) / S) / (N - lambda * F)
          } else {
            v = (S - lambda * SPARSNESS_CT) / miu
          }

          error += Math.abs(v - condTransducer.getFromIntIndexes(a, b))
          condTransducer.set(a, b, v)
          println("c(" + bb + "|" + aa + ") = " + condTransducer.getFromIntIndexes(a, b))
        }
      }
      
      condTransducer.checkNormalizationConditions_1_2_3
    }
    
    /*
    // Output results
    thetas.map{ case (ent, namesMap) => Utils.toString_ThetasMapForOneEntity(ent, namesMap) }
    	.saveAsTextFile(args(1))
   	*/
    sc.stop()
  }
  
}
