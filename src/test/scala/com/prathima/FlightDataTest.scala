package com.prathima

import com.prathima.FlightData.{computeFlightRoutes, countNumberofFlights, flownTogether, passengerFlewTogether, topHundredMostFrequentFlyers}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.{AnyFunSuiteLike, FunSuite}
import org.scalatest.BeforeAndAfterAll
import org.scalatest._
import flatspec._
import org.apache.log4j.{Level, Logger}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.sql.Date

trait SparkTester {

  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("test spark app")
      .getOrCreate()
  }
}
case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)
case class FlightsCount(month: String, numberofFlights: Long)
case class TopMostFrequentFlyers(passengerId: Int,numberofFlights: Long,firstName: String,lastName:String)
case class Passengers(passengerId: Int, firstName: String, lastName: String)
case class FinalResults(passengerId: Int, routes: String,longestRouteWithoutUK: Int)
case class PassengersFlewTogether(passenger1Id: Int,passenger2Id:Int,numberofFlightsTogether: Long)
case class PassengersFlewTogetherDateRange(passenger1Id: Int, passenger2Id: Int,numberofFlightsTogether: Long, from: String, to:String)
class FlightDataTest extends AnyFunSuiteLike with SparkTester with BeforeAndAfterAll {

  import spark.implicits._
  @transient var myDS:Dataset[Flight] = _
  @transient var myPassengersDS:Dataset[Passengers]= _
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  override def beforeAll(): Unit = {


    val myData = Seq(

      Flight(34, 0, "cg", "ir", "2017-01-01"),
      Flight(3, 0, "cg", "ir", "2017-01-01"),
      Flight(8035, 424, "cn", "th", "2017-05-27"),
      Flight(1923, 424, "cn", "th", "2017-05-27"),
      Flight(6146, 796, "cl", "jo", "2017-10-18"),
      Flight(8317, 796, "cl", "jo", "2017-10-18"),
      Flight(11239, 796, "cl", "jo", "2017-10-18"),
      Flight(2068, 26, "se", "ir", "2017-01-09"),
      Flight(2068, 32, "ir", "sg", "2017-01-10"),
      Flight(2068, 90, "sg", "nl", "2017-01-29"),
      Flight(2068, 125, "nl", "tj", "2017-02-13"),
      Flight(2068, 129, "tj", "tj", "2017-02-15"),
      Flight(2068, 173, "tj", "dk", "2017-03-03"),
      Flight(2068, 175, "dk", "pk", "2017-03-04"),
      Flight(2068, 234, "pk", "tj", "2017-03-24"),
      Flight(2068, 237, "tj", "tj", "2017-03-25"),
      Flight(2068, 323, "tj", "th", "2017-04-26"),
      Flight(2068, 392, "th", "no", "2017-05-17"),
      Flight(2068, 448, "no", "tk", "2017-06-06"),
      Flight(2068, 451, "tk", "pk", "2017-06-08"),
      Flight(2068, 458, "pk", "tj", "2017-06-11"),
      Flight(2068, 459, "tj", "no", "2017-06-12"),
      Flight(2068, 473, "no", "dk", "2017-06-18"),
      Flight(2068, 487, "dk", "ar", "2017-06-24"),
      Flight(2068, 503, "ar", "nl", "2017-06-28"),
      Flight(2068, 575, "nl", "us", "2017-07-24"),
      Flight(2068, 594, "us", "uk", "2017-08-01"),
      Flight(2068, 608, "uk", "co", "2017-08-06"),
      Flight(2068, 715, "co", "iq", "2017-09-18"),
      Flight(275, 2, "ca", "cn", "2017-01-01"),
      Flight(275, 48, "cn", "at", "2017-01-13"),
      Flight(275, 57, "at", "pk", "2017-01-17"),
      Flight(275, 70, "pk", "pk", "2017-01-21"),
      Flight(275, 78, "pk", "tj", "2017-01-24"),
      Flight(275, 85, "tj", "au", "2017-01-26"),
      Flight(275, 102, "au", "dk", "2017-02-03"),
      Flight(275, 165, "dk", "ar", "2017-02-27"),
      Flight(275, 245, "ar", "cl", "2017-03-28"),
      Flight(275, 272, "cl", "tj", "2017-04-09"),
      Flight(275, 283, "tj", "fr", "2017-04-13"),
      Flight(275, 658, "fr", "pk", "2017-08-25"),
      Flight(275, 926, "pk", "iq", "2017-12-08"),
      Flight(242, 2, "ca", "cn", "2017-01-01"),
      Flight(242, 48, "cn", "at", "2017-01-13"),
      Flight(242, 57, "at", "pk", "2017-01-17"),
      Flight(242, 104, "pk", "no", "2017-02-04"),
      Flight(242, 109, "no", "fr", "2017-02-07"),
      Flight(242, 126, "fr", "ir", "2017-02-13"),
      Flight(242, 182, "ir", "jo", "2017-03-07"),
      Flight(242, 195, "jo", "cl", "2017-03-11"),
      Flight(242, 272, "cl", "tj", "2017-04-09"),
      Flight(242, 721, "tj", "pk", "2017-09-19"),
      Flight(242, 877, "pk", "pk", "2017-11-21"),
      Flight(382, 3, "ch", "tj", "2017-01-01"),
      Flight(382, 12, "tj", "jo", "2017-01-03"),
      Flight(382, 18, "jo", "uk", "2017-01-04"),
      Flight(382, 34, "uk", "ar", "2017-01-10"),
      Flight(382, 45, "ar", "ch", "2017-01-12"),
      Flight(382, 133, "ch", "sg", "2017-02-17"),
      Flight(382, 149, "sg", "se", "2017-02-23"),
      Flight(382, 411, "se", "se", "2017-05-22"),
      Flight(382, 418, "se", "au", "2017-05-24"),
      Flight(382, 465, "au", "nl", "2017-06-15"),
      Flight(382, 477, "nl", "se", "2017-06-20"),
      Flight(382, 489, "se", "no", "2017-06-24"),
      Flight(382, 518, "no", "us", "2017-07-04"),
      Flight(382, 534, "us", "ca", "2017-07-11"),
      Flight(382, 576, "ca", "jo", "2017-07-24"),
      Flight(382, 611, "jo", "jo", "2017-08-08"),
      Flight(382, 624, "jo", "cn", "2017-08-12"),
      Flight(382, 631, "cn", "au", "2018-08-15"),
      Flight(376, 3, "ch", "tj", "2017-01-01"),
      Flight(376, 12, "tj", "jo", "2017-01-03"),
      Flight(376, 18, "jo", "uk", "2017-01-04"),
      Flight(376, 34, "uk", "ar", "2017-01-10"),
      Flight(376, 45, "ar", "ch", "2017-01-12"),
      Flight(376, 133, "ch", "sg", "2017-02-17"),
      Flight(376, 149, "sg", "se", "2017-02-23"),
      Flight(376, 261, "se", "cn", "2017-04-06"),
      Flight(376, 440, "cn", "tj", "2017-06-03"),
      Flight(376, 515, "tj", "tj", "2017-07-04"),
    )


    myDS = spark.createDataFrame(myData).as[Flight]

    val passengerData = Seq(
      Passengers(2068, "Yolande", "Pete"),
      Passengers(3, "Cameron", "Teddy"),
      Passengers(34, "Anastacia", "Consuelo"),
      Passengers(8035, "Katrice", "Theda"),
      Passengers(1923, "Porsha", "Analisa"),
      Passengers(6146, "Cheree", "Katia"),
      Passengers(8317, "Sarina", "Gigi"),
      Passengers(11239, "Hipolito", "Dario")

    )
    myPassengersDS = spark.createDataFrame(passengerData).as[Passengers]
  }

  override def afterAll(): Unit = {
  spark.stop()
  }

  test("testFlownTogether") {
    val result: Dataset[PassengersFlewTogetherDateRange] = flownTogether(myDS, 5, Date.valueOf("2017-01-01"), Date.valueOf("2017-3-01"))(spark)
    println("In function flownTogether")
    val expected = Seq(
      PassengersFlewTogetherDateRange(376, 382, 7, "2017-01-01", "2017-02-23"))
    val expectedResult = spark.createDataFrame(expected).as[PassengersFlewTogetherDateRange]
    assert(expectedResult.collect().sortBy(_.numberofFlightsTogether).sortBy(_.passenger1Id) === result.collect().sortBy(_.numberofFlightsTogether).sortBy(_.passenger1Id))
    result.show()

  }

  test("testTopHundredMostFrequentFlyers") {
    val result: Dataset[TopMostFrequentFlyers] = topHundredMostFrequentFlyers(myDS: Dataset[Flight], myPassengersDS)(spark)
    println("In function topHundredMostFrequentFlyers")
    val expected = Seq(
      TopMostFrequentFlyers(2068, 22, "Yolande", "Pete"),
      TopMostFrequentFlyers(3, 1, "Cameron", "Teddy"),
      TopMostFrequentFlyers(34, 1, "Anastacia", "Consuelo"),
      TopMostFrequentFlyers(8035, 1, "Katrice", "Theda"),
      TopMostFrequentFlyers(6146, 1, "Cheree", "Katia"),
      TopMostFrequentFlyers(8317, 1, "Sarina", "Gigi"),
      TopMostFrequentFlyers(11239, 1, "Hipolito", "Dario"),
      TopMostFrequentFlyers(1923, 1, "Porsha", "Analisa"),

    )
    //result.show()
    val expectedResult = spark.createDataFrame(expected).as[TopMostFrequentFlyers]
    assert(expectedResult.collect().sortBy(_.numberofFlights).sortBy(_.firstName) === result.collect().sortBy(_.numberofFlights).sortBy(_.firstName))
  }



  test("testCountNumberofFlights") {
    val result: Dataset[FlightsCount] = countNumberofFlights(myDS: Dataset[Flight])(spark)
    println("In function countNumberofFlights")
    // Create the expected result
    val expected = Seq(
      FlightsCount("01", 24),
      FlightsCount("02", 11),
      FlightsCount("03", 7),
      FlightsCount("04", 5),
      FlightsCount("05", 5),
      FlightsCount("06", 11),
      FlightsCount("07", 5),
      FlightsCount("08", 6),
      FlightsCount("09", 2),
      FlightsCount("10", 3),
      FlightsCount("11", 1),
      FlightsCount("12", 1)
    )
    val expectedResult = spark.createDataFrame(expected).as[FlightsCount]
    //result.show()
    // Collect and compare the results
    assert(expectedResult.collect().sortBy(_.month) === result.collect().sortBy(_.month))


  }

  test("testPassengerFlewTogether") {
    val result: Dataset[PassengersFlewTogether] = passengerFlewTogether(myDS)(spark)
    println("In function Passenger Flew Together")
    val expected = Seq(
      PassengersFlewTogether(242, 275, 4),
      PassengersFlewTogether(376, 382, 7))
    val expectedResult = spark.createDataFrame(expected).as[PassengersFlewTogether]
    assert(expectedResult.collect().sortBy(_.numberofFlightsTogether) === result.collect().sortBy(_.numberofFlightsTogether))

    //result.show()
  }

  test("testComputeFlightRoutes") {
    val result: Dataset[FinalResults] = computeFlightRoutes(myDS)(spark)
    println("In function ComputeFlightRoutes")
     val expected = Seq(
       FinalResults(2068,"se->ir->sg->nl->tj->tj->dk->pk->tj->tj->th->no->tk->pk->tj->no->dk->ar->nl->us->uk->co",20),
       FinalResults(382,"ch->tj->jo->uk->ar->ch->sg->se->se->au->nl->se->no->us->ca->jo->jo->cn",14),
       FinalResults(275,"ca->cn->at->pk->pk->tj->au->dk->ar->cl->tj->fr->pk",13),
       FinalResults(242,"ca->cn->at->pk->no->fr->ir->jo->cl->tj->pk",11),
       FinalResults(376,"ch->tj->jo->uk->ar->ch->sg->se->cn->tj",6),
       FinalResults(34,"cg",1),
       FinalResults(6146,"cl",1),
       FinalResults(3,"cg",1),
       FinalResults(11239,"cl",1),
       FinalResults(1923,"cn",1),
       FinalResults(8035,"cn",1),
       FinalResults(8317,"cl",1)
     )

    val expectedResult = spark.createDataFrame(expected).as[FinalResults]
    assert(expectedResult.collect().sortBy(_.longestRouteWithoutUK).sortBy(_.passengerId) === result.collect().sortBy(_.longestRouteWithoutUK).sortBy(_.passengerId))
    result.show()
  }

}
