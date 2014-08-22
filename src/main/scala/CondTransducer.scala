import scala.collection.mutable.HashMap

class CondTransducer extends Serializable {
  // Eps defining insertion or deletion operation. 
  val eps = '\0'

  // C contains the primitive conditional probabilties c(a|b).
  private var c = new HashMap[Char, HashMap[Char, Double]]

  // Returns c(b|a).
  def get(a : Char, b : Char) : Double = {
    if (!c.contains(a)) {
      if (a == b) return 1.0
      else return 0.0
    }
    
    if (!c(a).contains(b)) {
      return 0.0;
    }
    c(a)(b)
  }
  
  // Simple dummy init for c(b|a).
  def initC_0 = {
	c = new HashMap[Char, HashMap[Char, Double]]

	val gamma = 0.2
	c += (eps -> new HashMap[Char, Double])
	c(eps) += (eps -> gamma)
	
	for (b <- '\040' to '\177') {
	  c(eps) += (b -> (1 - c(eps)(eps)) / 96)
	}
	
	for (a <- '\040' to '\177') {
	  c += (a -> new HashMap[Char, Double])
	  for (b <- '\040' to '\177') {
	    c(a) += (b -> c(eps)(eps) / 97)
	  }
	  c(a) += (eps -> c(eps)(eps) / 97)
	}
	checkNormalizationConditions_1_2_3
  }
  
  // Check that equations 1,2,3 from Oncina paper are satisfied.
  def checkNormalizationConditions_1_2_3 = {
    var result = true

    assert(c.contains(eps), "[ERROR] Beta does not contain eps")
    var sum_3 = 0.0
    for ((b, prob) <- c(eps)) {
      sum_3 += prob
    }
    assert(Math.abs(sum_3 - 1.0) < 0.001, "[ERROR] Beta is not correct for eps with sum = " + sum_3)
          
    for ((a, hashmap) <- c) {
      assert(c(a).contains(eps), "[ERROR] Beta(" + a + ") does not contain eps")
      if (a != eps) {
        var sum_2 = 0.0
        for ((b, prob) <- c(a)) {
          sum_2 += prob
        }
        sum_2 += 1 - c(eps)(eps)
        assert(Math.abs(sum_2 - 1.0) < 0.001, "[ERROR] Beta is not correct for char " + a + " with sum = " + sum_2)
      }
    }
  }
  
}