import Array._
import Utils.convertChar

class CondTransducer extends Serializable {
  // Eps defining insertion or deletion operation. 
  val eps = '\0'

  val init_gamma = 0.5

  // C contains the primitive conditional probabilities c(a|b).
  private var c = ofDim[Double](97, 97)
  
  // Returns c(b|a).
  def get(a : Char, b : Char) : Double = {
    val aa = convertChar(a)
    val bb = convertChar(b)
    getFromIntIndexes(aa,bb)
  }

  def getFromIntIndexes(aa : Int, bb : Int) : Double = {
    if (aa < 0 || aa >= 97 || bb < 0 || bb >= 97) {
      if (aa == bb) return 1.0
      else return 0.0
    }
    c(aa)(bb)
  }  
  
  def set(aa: Int, bb: Int, v: Double) = {
    if (aa >= 0 && aa < 97 && bb >= 0 && bb < 97) {
      c(aa)(bb) = v
    }
  }
  
  // Simple dummy init for c(b|a).
  def initC_0 = {
	c = ofDim[Double](97, 97)

	c(convertChar(eps))(convertChar(eps)) = init_gamma
	
	for (b <- '\040' to '\177') {
	  c(convertChar(eps))(convertChar(b)) = (1 - init_gamma) / 96
	}
	
	for (a <- '\040' to '\177') {
	  c(convertChar(a))(convertChar(a)) = init_gamma / 2
	  for (b <- '\040' to '\177') {
	    if (b != a) {
	      c(convertChar(a))(convertChar(b)) = (init_gamma - c(convertChar(a))(convertChar(a))) / 96	      
	    }
	  }
	  c(convertChar(a))(convertChar(eps)) = (init_gamma - c(convertChar(a))(convertChar(a))) / 96
	}

	checkNormalizationConditions_1_2_3
  }
  
  // Check that equations 1,2,3 from Oncina paper are satisfied.
  def checkNormalizationConditions_1_2_3 = {
    var result = true

    var sum_3 = 0.0
    for (b <- 0 to 96) {
      sum_3 += c(0)(b)
    }
    assert(Math.abs(sum_3 - 1.0) < 0.001, "[ERROR] Beta is not correct for eps with sum = " + sum_3)
          
    for (a <- 1 to 96) {
        var sum_2 = 0.0
        for (b <- 0 to 96) {
          sum_2 += c(a)(b)
        }
        sum_2 += 1 - c(0)(0)
        assert(Math.abs(sum_2 - 1.0) < 0.001, "[ERROR] Beta is not correct for char " + a + " with sum = " + sum_2)
    }
  }
  
}