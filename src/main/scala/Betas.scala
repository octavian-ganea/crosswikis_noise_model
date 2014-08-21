import scala.collection.mutable.HashMap

object Betas {
  // '\0' will be epsilon. 
  private var delta = new HashMap[(Char, Char), Double]()

  def initDelta = {
    
  }
  
  
  // Corruption model p(x|m; beta)
  def p_x_cond_m_beta(x : String, m : String) : Double = {
    var rez = 1.0
    for (i <- 0 to Math.min(x.size,m.size) - 1) {
      rez *= 10
      if (x(i) == m(i))
        rez *= 0.5
      else if (x(i) >= 32 && x(i) < 128)
        rez *= 0.5/97
      else // Don't support replacement with other chars except from [32,127] for the moment.
        rez = 0
    }
    
    for (i <- Math.min(x.size,m.size) to Math.max(x.size,m.size) - 1) {
      rez *= 0.5/97
    }
    rez
  }
}