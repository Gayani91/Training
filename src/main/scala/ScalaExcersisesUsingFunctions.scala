import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source

object _5ReadAirports_Task2 {

  //Static varaibles
  val fileContents = Source.fromFile("/home/ayoobm/IdeaProjects/HelloWorld/src/airports.text").getLines.toList

  val splittedData = fileContents.map(line => {
    val airportID = line.split(",")(0)//Airport ID
    val airportName = line.split(",")(1)//Airport Name
    val city = line.split(",")(2)//Airport City
    val country = line.split(",")(3)//Country
    val iata = line.split(",")(4)//IATA code
    val icao = line.split(",")(5)//ICAO code
    val latitude = line.split(",")(6)//Latitude
    val longitude = line.split(",")(7)//Longitude
    val altitude = line.split(",")(8)//Altitude
    val timezone = line.split(",")(9)//Timezone
    val dst = line.split(",")(10)//DST
    val timezoneInOlson = line.split(",")(11)//Timezone in Olson
    (airportID,airportName,city,country,iata,icao,latitude,longitude,altitude,timezone,dst,timezoneInOlson)//Tuples
  })//Convert to list cuz iterators get empty after the first traversal.

  def main(args: Array[String]): Unit = {
    val split = fileContents.map(line => {
      val a = line.split(",")(1)//Airport Name
      val b = line.split(",")(3)//Country
      (a,b)
    }).groupBy(line => line._2).mapValues(line => line.map(r=>r._1))

    for ((country,list) <- split)
      println(country,list)
  }

  //Number of AirPorts in a country
  def numberOfAirportsInCountry(country: String): Unit ={
    val split = fileContents.map(r => r.split(",")(3)).toList.count(row => row.contains("\""+country+"\""))
    if (split > 0) println(country+" has "+split+" Airports.") else println(country+" has no Airports")
  }

  //Count of each airport above 40 altitude
  def countEachAPAbove40(): Unit ={
    val numberOf = splittedData.count(r=>r._7 > "40")
    println("Count : "+numberOf)
  }

    //Write to file above 40
  def fileWriteAbove40(): Unit ={
    val file = new File("airports_by_latitude.text")
    val bw = new BufferedWriter(new FileWriter(file))

    val numberOf = splittedData.foreach(line => {
      if (line._7 > "40"){
        bw.write(line._2+" : "+line._7+"\n")
      }
    })
    bw.close()
  }

  //Write all the airports in USA
  def writeAmericanAP(): Unit ={
    val splitted = splittedData.foreach(line =>{
      if (line._4 == "\"United States\"")
        println(line._2+" : "+line._3)
    })
  }

}

