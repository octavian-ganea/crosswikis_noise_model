import org.apache.spark.broadcast.Broadcast

// The parameters of the conditional transducers as described in the Oncina paper
object Betas {
  
  // Corruption model p(y|x; beta) = alpha(y|x) * gamma
  // Time complexity: O(y.size * x.size)
  def p_y_cond_x(y : String, x : String, c : CondTransducer) : Double = {
    alpha(y,x, c) * c.get(c.eps,c.eps)
  }
  
  // Forward alpha function as defined in page 9 of Oncina paper, but using dynamic prog.
  // Dynamic programming: Precompute alpha and beta values to reduce time complexity.
  // Time complexity: O(y.size * x.size)
  def alpha(y : String, x : String, c : CondTransducer) : Double = {
    val alphaMatrix = alphaMat(y, x, c)
    alphaMatrix(x.size)(y.size)
  }
  
  def alphaMat(y : String, x : String, c : CondTransducer) : Array[Array[Double]] = {
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
    alphaMatrix
  }

  // Backward beta function as defined in page 9 of Oncina paper.
  // Time complexity: O(y.size * x.size)  
  def beta(y : String, x : String, c : CondTransducer) : Double = {
    val betaMatrix = betaMat(y, x, c)
    betaMatrix(0)(0)
  }
  
  def betaMat(y : String, x : String, c : CondTransducer) : Array[Array[Double]] = {
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
    betaMatrix
  }  

  
  // Compute F(a) = \sum_m num_m_e * num_a_in_m
  def computeUnormalizedFFs(
      namesTheta : Array[(String, Int, Double)]) : Array[Double] = {

    var ff = Array.fill[Double](96)(0)
    
    for ((y, num_y_e, theta) <- namesTheta) {
      for (a <- y) {
        var aa : Int = Utils.convertChar(a)
        if (aa > 0 && aa < 96) {
          ff(aa) += num_y_e
        }
      }
    }
    ff
  }  
  
  // The expectation step from the Oncina paper: compute delta(b|a) using dynamic programming.
  def computeDeltas(
      namesThetaUncorrupted : Array[(String, String, Int, Double)], cc : Broadcast[CondTransducer]) : Array[Array[Double]] = {
    
    val c = cc.value
    var mat = Array.fill[Double](96,96)(0.0)
    
    for ((y, x, num_y_e, theta) <- namesThetaUncorrupted) {
      if (x.size > 0 && y.size > 0) {
        
        val alphaMatrix = alphaMat(y, x, c)
        var betaMatrix = betaMat(y, x, c)
        
        assert(Math.abs(alphaMatrix(x.size)(y.size) - betaMatrix(0)(0)) < 0.00001, "Alpha and beta matrices are wrong for x=" + x + " ; y= " + y)
        
        var p_y_condi_x = alphaMatrix(x.size)(y.size)
        assert(!p_y_condi_x.isNaN, "p(y|x) is NaN for x = " + x + " and y = " + y)
        
        
        // Compute delta(b|a) where b != eps, a != eps
        for (ix <- 0 to x.size - 1) {
          for (iy <- 0 to y.size - 1) {
            val a = x(ix)
            val b = y(iy)
            val aa = Utils.convertChar(a)
            val bb = Utils.convertChar(b)
            if (aa < 96 && bb < 96 && aa > 0 && bb > 0) {
              mat(aa)(bb) += num_y_e * (c.get(a, b) * alphaMatrix(ix)(iy) / p_y_condi_x) * betaMatrix(ix + 1)(iy + 1)
              assert(mat(aa)(bb) >= 0, "mat(" + a + "," + b + ") is negative : " + mat(aa)(bb) + " " + (num_y_e * (c.get(a, b) * alphaMatrix(ix)(iy) / p_y_condi_x) * betaMatrix(ix + 1)(iy + 1)))  // PROBLEM HERE 
            }
          }
        }
      
        // Compute delta(eps|a) where a != eps
        for (ix <- 0 to x.size - 1) {
          for (iy <- 0 to y.size) {
            val a = x(ix)
            val aa = Utils.convertChar(a)
            if (aa < 96 && aa > 0) {
              mat(aa)(0) += num_y_e * (c.get(a, c.eps) * alphaMatrix(ix)(iy) / p_y_condi_x) * betaMatrix(ix + 1)(iy)
              assert(mat(aa)(0) >= 0, "mat(" + a + ",eps ) is negative : " + mat(aa)(0))    // PROBLEM HERE                 
            }
          }
        }
      
        // Compute delta(b | eps) where b != eps
        for (ix <- 0 to x.size) {
          for (iy <- 0 to y.size - 1) {
            val b = y(iy)
            val bb = Utils.convertChar(b)
            if (bb < 96 && bb > 0) {
              mat(0)(bb) += num_y_e * (c.get(c.eps, b) * alphaMatrix(ix)(iy) / p_y_condi_x) * betaMatrix(ix)(iy + 1)
              assert(mat(0)(bb) >= 0, "mat(eps," + b + ") is negative : " + mat(0)(bb))     // PROBLEM HERE               
            }
          }
        }
      
        mat(0)(0) += num_y_e       
      }
    }
    mat
  }
  
}