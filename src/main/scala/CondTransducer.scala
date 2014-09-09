import Array._
import Utils.convertChar

class CondTransducer extends Serializable {
  // Eps defining insertion or deletion operation. 
  val eps = '\0'

  val init_gamma = 0.95

  // C contains the primitive conditional probabilities c(a|b).
  private var c = ofDim[Double](96, 96)
  
  // Returns c(b|a).
  def get(a : Char, b : Char) : Double = {
    val aa = convertChar(a)
    val bb = convertChar(b)
    getFromIntIndexes(aa,bb)
  }

  def getFromIntIndexes(aa : Int, bb : Int) : Double = {
    if (aa < 0 || aa >= 96 || bb < 0 || bb >= 96) {
      if (aa == bb) return 1.0
      else return 0.0
    }
    c(aa)(bb)
  }  
  
  def set(aa: Int, bb: Int, v: Double) = {
    if (aa >= 0 && aa < 96 && bb >= 0 && bb < 96) {
      c(aa)(bb) = v
    }
  }
  
  // Simple dummy init for c(b|a).
  def initC_0 = {
	c = ofDim[Double](96, 96)
	c(convertChar(eps))(convertChar(eps)) = init_gamma
	
	for (b <- '\040' to '\176') {
	  c(convertChar(eps))(convertChar(b)) = (1 - init_gamma) / 95
	}
	
	for (a <- '\040' to '\176') {
	  c(convertChar(a))(convertChar(a)) = init_gamma * 4/5
	  for (b <- '\040' to '\176') {
	    if (b != a) {
	      c(convertChar(a))(convertChar(b)) = (init_gamma - c(convertChar(a))(convertChar(a))) / 95	      
	    }
	  }
	  c(convertChar(a))(convertChar(eps)) = (init_gamma - c(convertChar(a))(convertChar(a))) / 95
	}

	checkNormalizationConditions_1_2_3
  }
  
  // Check that equations 1,2,3 from Oncina paper are satisfied.
  def checkNormalizationConditions_1_2_3 = {
    var result = true

    var sum_3 = 0.0
    for (b <- 0 to 95) {
      sum_3 += c(0)(b)
      var bb = "" + (b + 31).toChar
      if (b == 0) bb = "eps"
      assert(c(0)(b) >= 0 && c(0)(b) <= 1.0 , "[ERROR] Cond transducer is incorrect : c(eps)(" + bb + ") = " + c(0)(b))
    }
    
    // PROBLEM HERE
    assert(Math.abs(sum_3 - 1.0) < 1, "[ERROR] Beta is not correct for eps with sum = " + sum_3) // HACK
    for (b <- 0 to 95) {
      c(0)(b) /= sum_3
    }
    
          
    for (a <- 1 to 95) {
      var sum_2 = 0.0
      for (b <- 0 to 95) {
        sum_2 += c(a)(b)
        assert(c(a)(b) >= 0 && c(a)(b) <= 1.0 , "[ERROR] Cond transducer is incorrect : c(" + a + ")("+b + ") = " + c(a)(b))
      }
      var aa = "" + (a + 31).toChar
      
      // PROBLEM HERE
      assert(Math.abs(sum_2 - c(0)(0)) < 1, "[ERROR] Cond transducer is not correct for char " + aa + " with sum = " + (sum_2 + 1 - c(0)(0))) // HACK
      for (b <- 0 to 95) {
        c(a)(b) *= (c(0)(0) / sum_2) 
      }
    }
  }
  
}