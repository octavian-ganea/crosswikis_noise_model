import org.apache.spark.broadcast.Broadcast

// The parameters of the conditional transducers as described in the Oncina paper
object Betas {
  
  // Corruption model p(y|x; beta) = alpha(y|x) * gamma
  // Time complexity: O(y.size * x.size)
  def p_y_cond_x(y : String, x : String, c : CondTransducer) : Double = {
    alpha(y,x, y.size, x.size, c) * c.get(c.eps,c.eps)
  }
  
  // Forward alpha function as defined in page 9 of Oncina paper, but using dynamic prog.
  // Time complexity: O(y.size * x.size)
  def alpha(y : String, x : String, y_stop : Int, x_stop : Int, c : CondTransducer) : Double = {
    var mat = Array.fill[Double](2, y.size + 1)(0)
    
    for (x_index <- 0 to x_stop) {
      for (y_index <- 0 to y_stop) {
        mat(1)(y_index) = 0.0
        if (x_index == 0 && y_index == 0) {
          mat(1)(y_index) += 1.0
        }        
        if (x_index > 0) {
          mat(1)(y_index) += mat(0)(y_index) * c.get(x(x_index - 1), c.eps)
        }
        if (y_index > 0) {
          mat(1)(y_index) += mat(1)(y_index - 1) * c.get(c.eps, y(y_index - 1))
        }
        if (x_index > 0 && y_index > 0) {
          mat(1)(y_index) += mat(0)(y_index - 1) * c.get(x(x_index - 1), y(y_index - 1))
        }
      }
      for (y_index <- 0 to y.size) {
        mat(0)(y_index) = mat(1)(y_index)
      }
    }
    mat(1)(y_stop)
  }

  // Backward beta function as defined in page 9 of Oncina paper.
  // Time complexity: O(y.size * x.size)  
  def beta(y : String, x : String, y_stop : Int, x_stop : Int, c : CondTransducer) : Double = {
    var mat = Array.fill[Double](2, y.size + 1)(0)
    
    for (x_index <- x.size to x_stop by -1) {
      for (y_index <- y.size to y_stop by -1) {
        mat(0)(y_index) = 0.0
        if (x_index == x.size && y_index == y.size) {
          mat(0)(y_index) += 1.0
        }
        if (x_index < x.size && y_index < y.size) {
          mat(0)(y_index) += mat(1)(y_index + 1) * c.get(x(x_index), y(y_index))
        }
        if (x_index < x.size) {
          mat(0)(y_index) += mat(1)(y_index) * c.get(x(x_index), c.eps)
        }
        if (y_index < y.size) {
          mat(0)(y_index) += mat(0)(y_index + 1) * c.get(c.eps, y(y_index))
        }
      }
      for (y_index <- 0 to y.size) {
        mat(1)(y_index) = mat(0)(y_index)
      }      
    }
    mat(0)(y_stop)
  }  

  
  // Compute \sum_m num_m_e * alpha(m|m)
  def computeIdenticalStringsMass(
      namesThetaUncorrupted : Array[(String, String, Int, Double)], cc : Broadcast[CondTransducer]) : Double = {
    var sum = 0.0
    for ((y, y_real, num_y_e, theta) <- namesThetaUncorrupted) {
      sum += num_y_e * alpha(y, y, y.size, y.size, cc.value)
    }
    sum
  }  
  
  // The expectation step from the Oncina paper: compute delta(b|a) using dynamic programming.
  def computeDeltas(
      namesThetaUncorrupted : Array[(String, String, Int, Double)], cc : Broadcast[CondTransducer]) : Array[Array[Double]] = {
    computeDeltasOrFFs(namesThetaUncorrupted, cc , true)
  }

  // Compute the unnormalized ff value for one entity: ff(b|a) = \sum_m num_m_e * \sum_{xax' = m , yby' = m} alpha(y|x) * c(b|a) * beta(y'|x') * gamma
  // This should be normalized by dividing each ff(b|a) to \sum_e \sum_m num_m_e
  def computeFFs(
      namesThetaUncorrupted : Array[(String, String, Int, Double)], cc : Broadcast[CondTransducer]) : Array[Array[Double]] = {
    computeDeltasOrFFs(namesThetaUncorrupted, cc , false)
  }
  
  def computeDeltasOrFFs(
      namesThetaUncorrupted : Array[(String, String, Int, Double)], cc : Broadcast[CondTransducer], compDeltas : Boolean) : Array[Array[Double]] = {
    
    val c = cc.value
    var mat = Array.fill[Double](97,97)(0)
    
    for ((y, y_real, num_y_e, theta) <- namesThetaUncorrupted) {
      val x = if (compDeltas) y_real else y

      if (x.size > 0 && y.size > 0) {        
        // Dynamic programming: Precompute alpha and beta values to reduce time complexity.
        
        // Forward alpha function as defined in page 9 of Oncina paper, but using dynamic prog.
        var alphaMatrix = Array.fill[Double](x.size + 1, y.size + 1)(0)
        for (ix <- 0 to x.size) {
          for (iy <- 0 to y.size) {
            if (ix == 0 && iy == 0) {
              alphaMatrix(ix)(iy) += 1.0
            }
            if (ix > 0) {
              alphaMatrix(ix)(iy) += alphaMatrix(ix - 1)(iy) * c.get(x(ix - 1), c.eps)
            }
            if (iy > 0) {
              alphaMatrix(ix)(iy) += alphaMatrix(ix)(iy - 1) * c.get(c.eps, y(iy - 1))
            }
            if (ix > 0 && iy > 0) {
              alphaMatrix(ix)(iy) += alphaMatrix(ix - 1)(iy - 1) * c.get(x(ix - 1), y(iy - 1))
            }
          }
        }

        // Backward beta function as defined in page 9 of Oncina paper.
        var betaMatrix = Array.fill[Double](x.size + 1, y.size + 1)(0)
        for (ix <- x.size to 0 by -1) {
          for (iy <- y.size to 0 by -1) {
            if (ix == x.size && iy == y.size) {
              betaMatrix(ix)(iy) += 1.0
            }
            if (ix < x.size && iy < y.size) {
              betaMatrix(ix)(iy) += betaMatrix(ix + 1)(iy + 1) * c.get(x(ix), y(iy))
            }
            if (ix < x.size) {
              betaMatrix(ix)(iy) += betaMatrix(ix + 1)(iy) * c.get(x(ix), c.eps)
            }
            if (iy < y.size) {
              betaMatrix(ix)(iy) += betaMatrix(ix)(iy + 1) * c.get(c.eps, y(iy))
            }
          }
        }
        
        var p_y_condi_x = if (compDeltas) {
          alphaMatrix(x.size)(y.size) * c.get(c.eps,c.eps)
        } else 1.0

        // Compute delta(b|a) where b != eps, a != eps
        for (ix <- 0 to x.size - 1) {
          for (iy <- 0 to y.size - 1) {
            val a = x(ix)
            val b = y(iy)
            val aa = Utils.convertChar(a)
            val bb = Utils.convertChar(b)
            if (aa < 97 && bb < 97 && aa > 0 && bb > 0) {
              mat(aa)(bb) += num_y_e * (c.get(a, b) * alphaMatrix(ix)(iy) / p_y_condi_x) * betaMatrix(ix + 1)(iy + 1) * c.get(c.eps , c.eps)
            }
          }
        }
      
        // Compute delta(eps|a) where a != eps
        for (ix <- 0 to x.size - 1) {
          for (iy <- 0 to y.size) {
            val a = x(ix)
            val aa = Utils.convertChar(a)
            if (aa < 97 && aa > 0) {
              mat(aa)(0) += num_y_e * (c.get(a, c.eps) * alphaMatrix(ix)(iy) / p_y_condi_x) * betaMatrix(ix + 1)(iy) * c.get(c.eps , c.eps)
            }
          }
        }
      
        // Compute delta(b | eps) where b != eps
        for (ix <- 0 to x.size) {
          for (iy <- 0 to y.size - 1) {
            val b = y(iy)
            val bb = Utils.convertChar(b)
            if (bb < 97 && bb > 0) {
              mat(0)(bb) += num_y_e * (c.get(c.eps, b) * alphaMatrix(ix)(iy) / p_y_condi_x) * betaMatrix(ix)(iy + 1) * c.get(c.eps , c.eps)
            }
          }
        }
      
        mat(0)(0) += num_y_e       
      }
    }
    mat
  }
  
}