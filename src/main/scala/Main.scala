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
  val CONV_EPS = 3 // HACK
  
  // Controls the percentage of probabilities that we allow for noisy strings : \sum_m p(m) * p(m|m;beta) = SPARSNESS_CT
  val SPARSNESS_CT = 0.6  
   
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
    
    	//.filter(line => (line.size > 0 && line.charAt(0) == 'B')) //TODO: remove this!!!)
    	
    	.map(line => { val ent = line.split("\t").head; (ent , line)} ) // Extract entity first
    	.groupByKey  // Group by entity
    	
    	.filter(x => (Math.random() <= 0.01)) //TODO: remove this!!!)
    	
    	.map{ case (ent, linesIter) => (ent, parseOneLineAndExtractMentionAndCounters(ent, linesIter)) }
    	.filter{ case (ent, namesMap) => namesMap.size > 0 } // we removed all n with #(n,e) <= 5

    var thetas = entNamesRDDMap
    	.mapValues{ namesMap => Thetas.computeInitialThetasForOneEnt(namesMap) }
    
    // Convergence error. Stop the iterative loop when error < CONV_EPS
    var error = 10000.0
    var iterationNum = 0
    
    while (error > CONV_EPS) {
      iterationNum += 1
      println("==========================================================================")
      println("================== Starting iteration " + iterationNum + " =================================")
      println("================== current error : " + error + " =================================")
      println("==========================================================================")
      
      val c = sc.broadcast(condTransducer)

      val thetasAux = thetas
      	.mapValues{ namesTheta0Map => Thetas.updateThetasForOneEnt(namesTheta0Map, c)}
      	
      thetas.unpersist(true)
      thetas = thetasAux.persist(StorageLevel.MEMORY_AND_DISK)
      
      val nameRepresentativesAndThetas = thetas
      	.mapValues{ namesTheta1Map => computeUncorruptedRepresentativeNames(namesTheta1Map, c)}
    
      val deltas = nameRepresentativesAndThetas
    	.map{ case (ent, namesThetaUncorrupted) => Betas.computeDeltas(namesThetaUncorrupted, c) }
      	.reduce((mat1, mat2) => Utils.addMatrices(mat1, mat2))

      for (a <- 0 to 95) {
        for (b <- 0 to 95) {
          var aa = "" + (a + 31).toChar
          var bb = "" + (b + 31).toChar
          if (a == 0) aa = "eps"
          if (b == 0) bb = "eps"
          assert(deltas(a)(b) >= 0, "delta(" + aa + "," + bb + ") is negative : " + deltas(a)(b))  // PROBLEM HERE      
          println("delta(" + aa + "," + bb + ") = " + deltas(a)(b))
        }
      }
      	
      val S = deltas(0)(0)

      // Compute N values
      var Narray = Array.fill[Double](96)(0)
      var N : Double = S
      for (a <- 0 to 95) {
        for (b <- 0 to 95) {
          if (a != 0 || b != 0) {
            Narray(a) += deltas(a)(b)
          }
        }
        assert(Narray(a) >= 0, "N(" + a + ") is negative : " + Narray(a))
        N += Narray(a)
      }

      // Compute F values
      val Farray = thetas
      	.map{ case (ent, namesThetaUncorrupted) => Betas.computeUnormalizedFFs(namesThetaUncorrupted)}
      	.reduce((vec1, vec2) => Utils.addVectors(vec1, vec2))
      	
      var F : Double = S
      for (a <- 1 to 95) {
        F += Farray(a)
        assert(Farray(a) >= 0, "F(" + a + ") is negative : " + Farray(a))

        var aa = "" + (a + 31).toChar
        if (a == 0) aa = "eps"
        println("F(" + aa + ") = " + Farray(a))
        println("N(" + aa + ") = " + Narray(a))
        println("delta(" + aa + "|" + aa + ") = " + deltas(a)(a))        
        println()
      }
      println("N(eps) = " + Narray(0))
      println ("F = " + F)
      println ("N = " + N)
      println ("S = " + S)
      println()


      val h_lambda : (Double => Double) = 
        (lambda => {
            // h(lambda) that should be equal to 0 in the end
            var result = - S * Math.log(SPARSNESS_CT)
            /*
            assert((N - Narray(0) -  lambda * (F/S)) * (N - lambda * (F/S)) >= 0,
                "Error in log computation for F = " + F + " ;N= " + N + " ;N(eps)=" + Narray(0) + "; lambda = " + lambda + 
                " log(N-N(eps)-lambda*F) = " + Math.log(N - Narray(0) -  lambda * (F/S)) + 
                " log(N-lambda*F) = " +Math.log(N - lambda * (F/S))) 
            */    
            if (N - Narray(0) -  lambda * (F/S) > 0) {
              result += F * (Math.log(N - Narray(0) -  lambda * (F/S)) - Math.log(N - lambda * (F/S)))
            } else if (N - Narray(0) -  lambda * (F/S) < 0) { // Can be 0.0
              result += F * (Math.log(- (N - Narray(0) -  lambda * (F/S))) - Math.log(-(N - lambda * (F/S))))              
            }
            for (a <- 1 to 95) {
              /*
              assert((deltas(a)(a) -  lambda * (Farray(a)/S)) * (Narray(a) - lambda * (Farray(a)/S)) >= 0,
                "Error in log computation for F(" + a + ") = " + Farray(a) + " ;N(" +a + ") = " + Narray(a) + " ;deltas(a)(a)=" + deltas(a)(a) + 
                "; lambda = "+ lambda + 
                " log(deltas(a)(a) -  lambda * F(a)) = " + Math.log(deltas(a)(a) -  lambda * (Farray(a)/S)) + 
                " log(N(a) - lambda * F(a)) = " +Math.log(Narray(a) - lambda * (Farray(a)/S)))
              */  
              if (deltas(a)(a) -  lambda * (Farray(a)/S) > 0) {
                result += Farray(a) * (Math.log(deltas(a)(a) -  lambda * (Farray(a)/S)) - Math.log(Narray(a) - lambda * (Farray(a)/S)))
              } else if (deltas(a)(a) -  lambda * (Farray(a)/S) < 0) { // Can be 0.0
                result += Farray(a) * (Math.log(- (deltas(a)(a) -  lambda * (Farray(a)/S))) - Math.log(-(Narray(a) - lambda * (Farray(a)/S))))
              }
            }
            result
          })
      
      val h_derivative_lambda : Double => Double = 
        (lambda => {
            var result = F * (- 1/((N - Narray(0))/F -  (lambda /S))  + 1/((N/F) - (lambda /S)))
            for (a <- 1 to 95) {
              result += Farray(a) * (- 1/((deltas(a)(a)/Farray(a)) -  (lambda /S)) + (1/((Narray(a)/Farray(a)) - (lambda /S))))
            }
            result
          })
       
      var lambda : Double = Double.NaN 
      var initLambda : Double = (N - Narray(0)) * (S / F)
      var e = 1.0
      for (i <- 1 to 10) {
        if (h_lambda(initLambda - e) <= 0) {
          lambda = initLambda - e
        }        
        e /= 10
      }    
      
      for (a <- 1 to 95) {
        if (Farray(a) > 0) {
          initLambda = (deltas(a)(a)) * (S / Farray(a))
          e = 1.0
          for (i <- 1 to 10) {
            if (h_lambda(initLambda - e) <= 0) {
              lambda = initLambda - e
            }
            e /= 10
          }           
        }
      }      
      
      assert(!lambda.isNaN, "That all folks")
      
      //val lambda = FindZerosOfFunctions.bisectionMethod(h_lambda, initialLambda0, initialLambda1, 0.001)
      //val lambda = FindZerosOfFunctions.newtonsMethod(h_lambda, h_derivative_lambda , initLambda, 0.0001)

      println ("lambda = " + lambda)
      
      val miu = ((N - lambda * (F/S)) / (N - Narray(0) -  lambda * (F/S))) * (S - lambda)
      var miuArray = Array.fill[Double](96)(0)
      for (a <- 1 to 95) {
        miuArray(a) = ((N - lambda * (F/S)) / (N - Narray(0) -  lambda * (F/S))) * (Narray(a) - lambda * (Farray(a)/S))
        //println("miu(" + a + ") = " + miuArray(a))
      }
      println ("miu = " + miu)
      
      println("Final results : ")
    
      error = 0.0
      for (a <- 0 to 95) {
        for (b <- 0 to 95) {
          var aa = "" + (a + 31).toChar
          var bb = "" + (b + 31).toChar
          if (a == 0) aa = "eps"
          if (b == 0) bb = "eps"
          var v = 0.0
          
          if (a != 0) {
            if (miuArray(a) > 0) {
              if (a != b) {
                v = deltas(a)(b) / miuArray(a)
              } else {
                v = (deltas(a)(a) - lambda * (Farray(a)/S)) / miuArray(a)
              }
            } else {
              if (a == b) v = 1.0
              else v = 0.0
            }
          } else if (b != 0) {
            v = deltas(0)(b) / (N - lambda * (F/S))
          } else {
            v = (S - lambda) / miu
          }
          error += Math.abs(v - condTransducer.getFromIntIndexes(a, b))
          condTransducer.set(a, b, v)
          
          if (a == b) {
            println("c(" + bb + "|" + aa + ") = " + condTransducer.getFromIntIndexes(a, b))
          }
        }
      }
      

      var sparsnessVal = - S * Math.log(SPARSNESS_CT) 
      sparsnessVal += S * Math.log(condTransducer.getFromIntIndexes(0,0))
      for (a <- 1 to 95) {
        sparsnessVal += Farray(a) * Math.log(condTransducer.getFromIntIndexes(a,a))
      }      
      println("End of iteration: Sparseness value = " + sparsnessVal)
      
      
      condTransducer.checkNormalizationConditions_1_2_3
    }
    
    // HACK : this should be an inner iterative EM algorithm
    println("==================  Final cond transducer probs: ======================== ")
    for (a <- 0 to 95) {
      for (b <- 0 to 95) {
        var aa = "" + (a + 31).toChar
        var bb = "" + (b + 31).toChar
        if (a == 0) aa = "eps"
        if (b == 0) bb = "eps"
        println("c(" + bb + "|" + aa + ") = " + condTransducer.getFromIntIndexes(a, b))
      }
    }
    
    // Output results
    val c = sc.broadcast(condTransducer)
    val nameRepresentativesAndThetas = thetas
      	.mapValues{ namesTheta1Map => computeUncorruptedRepresentativeNames(namesTheta1Map, c)}
    	.map{ case (ent, namesMap) => Utils.toString_ThetasMapForOneEntity(ent, namesMap) }
    	.saveAsTextFile(args(1))
    
    println("==========================================================================")
    println("================== Algorithm finished in " + iterationNum + " iters =================================")
    println("==========================================================================")    	
    sc.stop()
  }
  
}
