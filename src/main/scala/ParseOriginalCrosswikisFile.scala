object ParseOriginalCrosswikisFile {
  // Extract (ent, name, #(e,n)) from one line of form <url><tab><cprob><space><string>[<tab><score>[<space><score>]*]
  // Keep just names with counters >= 5
  def parseOneLineAndExtractMentionAndCounters(ent: String, linesIterator: Iterable[String]) : Array[(String, Int)] = {
    var mentionCountsArray = Array.empty[(String, Int)]
    for (line <- linesIterator) {
      val terms = line.split("\t")
      if (terms.size != 3) {
        throw new Exception("Crosswikis line is not well formated: " + line)
      }
      
      val name = terms(1).substring(terms(1).indexOf(' ') + 1)
      
      var numNameEnt = 0
      for (serialScore <- terms(2).split(" ")) {
        val reducedSerialScore = serialScore.split(":")
        if (reducedSerialScore.head != "Wx") {
          numNameEnt += reducedSerialScore.last.split("/").head.toInt
        }
      }
      if (numNameEnt >= 5 && !Utils.isGeneralPlaceholder(ent, name) ) {
    	  mentionCountsArray :+= (name, numNameEnt)
      }
    }
    // Sort array descending by the counts of the name
    mentionCountsArray.sortWith( (x, y) => x._2 > y._2)
  }
}