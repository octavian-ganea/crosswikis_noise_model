import scala.collection.mutable.HashMap
import Betas.p_x_cond_m_beta

object Thetas {
  // theta^0_{n,e} = #(n,e)/(\sum_n #(n,e))
  // Input: namesMap - array of pairs (name, #(n,e))
  // Output: hashmap {m : (#(m,e), theta0_m)}    
  def computeInitialThetasForOneEnt(namesMap : Array[(String, Int)]) : HashMap[String, (Int, Double)] = {
    var total_num_e = 0.0 
	for ((name, counter) <- namesMap) {
	  total_num_e += counter
	}
    val thetasArray = new HashMap[String, (Int, Double)]()
	for ((name, counter) <- namesMap) {
	  thetasArray += ((name, (counter, counter / total_num_e)))
	}    
    thetasArray
  }
  
  
  // Computes theta^1_{m,e} \propto  \sum_n #(n,e) * \frac {\theta^(0)_m * p(n|m;beta)} {\sum_m' \theta^(0)_m' * p(n|m';beta)}
  // Input: currentThetasForOneEntMap - hashmap {m : (#(m,e), theta0_m)}
  // Output: hashmap {m : (#(m,e), theta1_m)}
  def updateThetasForOneEnt(curThetasForOneEnt : HashMap[String, (Int, Double)]) : HashMap[String, (Int, Double)] = {
    
    // Compute numitorsMap(n) = \sum_m' \theta^(0)_m' * p(n|m';beta)
    val numitorsMap : HashMap[String, Double] = new HashMap()
    for ((n, (num_n_e, theta_n_0)) <- curThetasForOneEnt) {
      var numitor_n = 0.0
	  for ((m_prim, (num_m_prim_e, theta_m_prim_0)) <- curThetasForOneEnt) {
	    numitor_n += theta_m_prim_0 * p_x_cond_m_beta(n, m_prim)
	  }
      numitorsMap += (n -> numitor_n)
    }

    // Compute unnormalized thetas^(1)
    val newThetasForOneEnt : HashMap[String, (Int, Double)] = HashMap()
    var Z_thetas_1 = 0.0
    for ((m, (num_m_e, theta_m_0)) <- curThetasForOneEnt) {
	  var theta_m_1 = 0.0
	  for ((n, (num_n_e, theta_n_0)) <- curThetasForOneEnt) {
	    theta_m_1 += num_n_e * theta_m_0 * p_x_cond_m_beta(n, m) / numitorsMap(n)
	  }
	  Z_thetas_1 += theta_m_1
	  newThetasForOneEnt += (m -> (num_m_e, theta_m_1))
	}
    
    // Normalize thetas:
    for ((m, (num_m_e, theta_m_1)) <- newThetasForOneEnt) {
      newThetasForOneEnt(m) =  (num_m_e, theta_m_1 / Z_thetas_1)
    }
    newThetasForOneEnt
  }
}