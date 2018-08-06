import org.scalatest._

class ScalaTest extends FlatSpec with Matchers {

  "The Method number " should "return 15 in values" in {
    val numb = ScalaFun.numberOfAirportsInCountry("Sri Lanka")
    numb should be (15)
  }
  "The Method count " should "return 3634 in values" in {
    val numb = ScalaFun.countEachAPAbove40()
    numb should be (3634)
  }
  "the method group " should " return 244 in length" in {
    val numb = ScalaFun.groupByCountry()
    numb should be (244)
  }
}