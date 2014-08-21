import scala.collection.mutable.HashMap

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
  def toString_ThetasMapForOneEntity(ent: String, namesMap : HashMap[String, (String, Int, Double)]) : String = {
    var output = ent + "\t==>\n" 
    for ((name, (realName, num_name_ent, theta_1)) <- namesMap) {
      if (name != realName) output += "**"
      output += "\t" + name + "\t ---> " + realName + "\t" + theta_1 + "\n"
    }
    if (namesMap.size == 0) ""
    else output
  }
}