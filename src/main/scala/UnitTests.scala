object UnitTests {

  def tests : Unit = {
	val c = new CondTransducer
	c.initC_0
    
	var x = "obladi"
	var y = "oblada"
	assert(Math.abs(Betas.alpha(y, x, c) - Betas.beta(y, x, c)) < 0.00001 , 
	    "Unit tests failed: " + y + " " + x + "; alpha = " + Betas.alpha(y, x, c) + "; beta=" + Betas.beta(y, x, c))
	   
	x = "a fost o data"
	y = "ca-n pov"
	assert(Math.abs(Betas.alpha(y, x, c) - Betas.beta(y, x, c)) < 0.00001 , 
	    "Unit tests failed: " + y + " " + x + "; alpha = " + Betas.alpha(y, x, c) + "; beta=" + Betas.beta(y, x, c))

	x = "Barack Obama"
	y = "Barak Obama'"
	assert(Math.abs(Betas.alpha(y, x, c) - Betas.beta(y, x, c)) < 0.00001 , 
	    "Unit tests failed: " + y + " " + x + "; alpha = " + Betas.alpha(y, x, c) + "; beta=" + Betas.beta(y, x, c))
	 	
	    
	assert(Betas.alpha("", "", c) == 1, "Fail empty strings test for alpha.")
	    
	println("Unittests passed succesfully!\n")
  }
}