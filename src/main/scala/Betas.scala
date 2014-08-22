import scala.collection.mutable.HashMap
import Array._

// The parameters of the conditional transducers as described in the Oncina paper
object Betas {
  
  // Corruption model p(y|x; beta) = alpha(y|x) * gamma
  def p_y_cond_x(y : String, x : String, c : CondTransducer) : Double = {
    //alpha(y,x,c) * c.get(c.eps,c.eps)  NU E DE AICI !! E DIN ALTA PARTE !!!!
    Math.random
  }
  

  // Forward alpha function as defined in page 9 of Oncina paper, but using dynamic prog.
  def alpha(y : String, x : String, c : CondTransducer) : Double = {
    var mat = ofDim[Double](x.size + 1, y.size + 1)
    
    for (x_index <- 0 to x.size) {
      for (y_index <- 0 to y.size) {
        mat(x_index)(y_index) = 0.0
        if (x_index == 0 && y_index == 0) {
          mat(x_index)(y_index) += 1.0
        }
        if (x_index > 0 && y_index > 0) {
          mat(x_index)(y_index) += mat(x_index - 1)(y_index - 1) * c.get(x(x_index - 1), y(y_index - 1))
        }
        if (x_index > 0) {
          mat(x_index)(y_index) += mat(x_index - 1)(y_index) * c.get(x(x_index - 1), c.eps)
        }
        if (y_index > 0) {
          mat(x_index)(y_index) += mat(x_index)(y_index - 1) * c.get(c.eps, y(y_index - 1))
        }
      }
    }
    mat(x.size)(y.size)
  }


  // Backward beta function as defined in page 9 of Oncina paper.
  def beta(y : String, x : String, c : CondTransducer) : Double = {
    var mat = ofDim[Double](x.size + 1, y.size + 1)
    
    for (x_index <- x.size to 0 by -1) {
      for (y_index <- y.size to 0 by -1) {
        mat(x_index)(y_index) = 0.0
        if (x_index == x.size && y_index == y.size) {
          mat(x_index)(y_index) += 1.0
        }
        if (x_index < x.size && y_index < y.size) {
          mat(x_index)(y_index) += mat(x_index + 1)(y_index + 1) * c.get(x(x_index), y(y_index))
        }
        if (x_index < x.size) {
          mat(x_index)(y_index) += mat(x_index + 1)(y_index) * c.get(x(x_index), c.eps)
        }
        if (y_index < y.size) {
          mat(x_index)(y_index) += mat(x_index)(y_index + 1) * c.get(c.eps, y(y_index))
        }
      }
    }
    mat(0)(0)
  }  

}