import java.util.Properties
import scala.io.Source

object PhoneFormat extends App {
  /**
    *
    * @param 998256748 , 1 – 33 1569 –352
    * @return 998-256-748, 133-156-93-52
    */

  def formatPhone (input: String) = {

  }
  def getConfig(filePath: String)= {
    Source.fromFile(filePath).getLines().filter(line => line.contains("=")).map{ line =>
      println(line)
      val tokens = line.split("=")
      ( tokens(0) -> tokens(1))
    }.toMap
  }
val props = getConfig("G:\\POC_HiveToHbase\\src\\main\\resources\\application.properties")

}

/*

      /*var cleanInput = input.replaceAll("[ -]+", "")
    var phone = new StringBuilder(cleanInput)

    var dash = (phone.length / 3) * 3

    phone.length % 3 match {
      case 0 =>
      //nothing to do for an exact grouping
      case 1 => phone.insert(dash - 1, '-')
      //insert the dash making 2-2 groups instead of 3-1
      case 2 => phone.insert(dash, '-')
    }

    while (dash > 3) {
      dash = dash - 3
      phone.insert(dash, '-')
    }
    return phone.toString()*/
//println(formatPhone("998256748"))
    //println(formatPhone("1 - 33 1569 --352"))

 */