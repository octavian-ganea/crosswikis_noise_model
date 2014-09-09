import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.Map

object Utils {
  
  // Remove names containing "Wikipedia", "wikipedia" or numbers ("[1]" , "[3]", etc)
  def isGeneralPlaceholder(ent : String, name : String) : Boolean = {
    if ((name.contains("Wikipedia") || name.contains("wikipedia")) && !ent.contains("Wikipedia") && !ent.contains("wikipedia")) 
      true
    else if (name.filter(!Character.isDigit(_)) == "[]")
      true
    else
      false
  }
  
  // Human readable output formatting.
  // Input: hashmap {m : (uncorrupted(m), #(m,e), theta0_m, theta1_m)}
  // Output: output text in a human readable format
  def toString_ThetasMapForOneEntity(ent: String, namesMap : Array[(String, String, Int, Double)]) : String = {
    var output = ent + "\t==>\n" 
    for ((name, realName, num_name_ent, theta_1) <- namesMap) {
      if (name != realName) output += "**"
      output += "\t" + name + "\t ---> " + realName + "\n"
    }
    if (namesMap.size == 0) ""
    else output
  }
  
  
  def convertChar(a : Char) : Int = {
    if (a == '\0') 0
    else a - 31
  }  
  
  
  def addMatrices(mat1 : Array[Array[Double]], mat2 : Array[Array[Double]]) : Array[Array[Double]] = {
    var mat = mat1.clone
    for (i <- 0 to 95) {
      for (j <- 0 to 95) {
        mat(i)(j) += mat2(i)(j)
      }
    }
    mat
  }

  def addVectors(vec1 : Array[Double], vec2 : Array[Double]) : Array[Double] = {
    var v = vec1.clone
    for (i <- 0 to 95) {
      v(i) += vec2(i)
    }
  	v
  }

}