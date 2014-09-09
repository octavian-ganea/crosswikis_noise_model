object FindZerosOfFunctions {
  /**
   * Newton's Method for solving equations.
   * TODO: check that |f(xNext)| is greater than a second tolerance value
   * TODO: check that f'(x) != 0
   */
  def newtonsMethod(fx: Double => Double, 
                    fxPrime: Double => Double,
                    x: Double,
                    tolerance: Double): Double = {
    println("*********** Starting Newton's method ************************")    
    var x1 = x
    println("Cur value = " + x1 + " where f(x) = " + fx(x1) + " ;f'(x) = " + fxPrime(x1) )

    var numSteps = 0
    while (/*Math.abs(xNext - x1)*/ Math.abs(fx(x1)) > tolerance) {
      x1 = computeNextValue(fx, fxPrime, x1)
      println("Cur value = " + x1 + " where f(x) = " + fx(x1) + " ;f'(x) = " + fxPrime(x1) )
      numSteps += 1 
    }
    assert(!x1.isNaN, "current value is Nan at iteration " + numSteps + " where prev value was " + x1)
    println("*********** Newton's method is finished in " + numSteps + " steps. With value = " + x1 + " where f(x) = " + fx(x1) + " ************************")
    return x1
  }
 
  /**
   * This is the "x2 = x1 - f(x1)/f'(x1)" calculation 
   */
  private def computeNextValue(fx: Double => Double, 
                          fxPrime: Double => Double,
                          x: Double): Double = {
    return x - fx(x) / fxPrime(x)
  }
  
  
  def bisectionMethod(fx: Double => Double, 
		  			  x0: Double,
		  			  x2: Double,
		  			  tolerance: Double): Double = {
    println("*********** Starting Bisection method ************************")    
    var xstart = x0 
    var xend = x2
    var numSteps = 0
    while (Math.abs(fx(xstart)) > tolerance) {
      val xmid = xstart + (xend - xstart)/2
      println("xstart " + xstart + " xend " + xend + " xmid " + xmid  + " has value " + fx(xmid))
      if (fx(xmid) * fx(xstart) < 0) {
        xend = xmid
      } else {
        xstart = xmid
      }
      numSteps += 1 
    }
    assert(!xstart.isNaN, "current value is Nan at iteration " + numSteps + " where prev value was " + xstart)
    println("*********** Bisection's method is finished in " + numSteps + " steps. With value = " + xstart + " where f(x) = " + fx(xstart) + " ************************")
    return xstart
  }
}