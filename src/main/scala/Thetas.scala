import org.apache.spark.broadcast.Broadcast
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.Map

import Betas.p_y_cond_x

object Thetas {
  // theta^0_{n,e} = #(n,e)/(\sum_n #(n,e))
  // Input: namesMap - array of pairs (name, #(n,e))
  // Output: hashmap {m : (#(m,e), theta0_m)}    
  def computeInitialThetasForOneEnt(namesMap : Array[(String, Int)]) : Array[(String, Int, Double)] = {
    var total_num_e = 0.0 
	for ((name, counter) <- namesMap) {
	  total_num_e += counter
	}
    val thetasArray = new Array[(String, Int, Double)](namesMap.size)
    var index = 0
	for ((name, counter) <- namesMap) {
	  thetasArray(index) = (name, counter, counter / total_num_e)
	  index = index + 1
	}    
    thetasArray
  }
  
  
  // Computes theta^1_{m,e} \propto  \sum_n #(n,e) * \frac {\theta^(0)_m * p(n|m;beta)} {\sum_m' \theta^(0)_m' * p(n|m';beta)}
  // Input: currentThetasForOneEntMap - hashmap {m : (#(m,e), theta0_m)}
  // Output: hashmap {m : (#(m,e), theta1_m)}
  def updateThetasForOneEnt(
      curThetasForOneEnt : Array[(String, Int, Double)], c : Broadcast[CondTransducer]) : Array[(String, Int, Double)] = {

    // Store all p(n|m) in a matrix to avoid calling the function p_y_cond_x many times.
    // This is just an optimization.
    val p_y_cond_x_map = new java.util.HashMap[(String, String), Double] 
  	for ((m, num_m_e, theta_m_0) <- curThetasForOneEnt) {
	  for ((n, num_n_e, theta_n_0) <- curThetasForOneEnt) {
	    p_y_cond_x_map += (n,m) -> p_y_cond_x(n, m, c.value)
	  }
  	}
    
    // Compute and store numitorsMap(n) = \sum_m' \theta^(0)_m' * p(n|m';beta)
    // This is just an optimization.    
    val numitorsMap = new java.util.HashMap[String, Double]
    for ((n, num_n_e, theta_n_0) <- curThetasForOneEnt) {
      var numitor_n = 0.0
	  for ((m_prim, num_m_prim_e, theta_m_prim_0) <- curThetasForOneEnt) {
	    numitor_n += theta_m_prim_0 * p_y_cond_x_map(n, m_prim)
	  }
      numitorsMap += n -> numitor_n
    }

    // Compute unnormalized thetas^(1)
    val newThetasForOneEnt = new Array[(String, Int, Double)](curThetasForOneEnt.size)
    var index = 0
    var Z_thetas_1 = 0.0
    for ((m, num_m_e, theta_m_0) <- curThetasForOneEnt) {
	  var theta_m_1 = 0.0
	  for ((n, num_n_e, theta_n_0) <- curThetasForOneEnt) {
	    theta_m_1 += num_n_e * theta_m_0 * p_y_cond_x_map(n, m) / numitorsMap(n)
	  }
	  Z_thetas_1 += theta_m_1
	  newThetasForOneEnt(index) = (m, num_m_e, theta_m_1)
	  index = index + 1
	}
    
    // Normalize thetas:
    for (index <- 0 to newThetasForOneEnt.size - 1) {
      newThetasForOneEnt(index) = (newThetasForOneEnt(index)._1, newThetasForOneEnt(index)._2, newThetasForOneEnt(index)._3 / Z_thetas_1)
    }

    newThetasForOneEnt
  }
}