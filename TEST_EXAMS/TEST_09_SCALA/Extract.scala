/// Extract Final
//Import Libararies
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import scala.util.parsing.json.JSON

object Extract {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Extract")
      .master("local[*]")
      .getOrCreate()

    val year: Int = 2021
    val totalPage: Int = 2  // sil sonra !!!!!!!!!!!!!!!!!!
    println(s"$year, $totalPage, Step 1 OK")


    // Teams API
    val r = requests.get("https://www.balldontlie.io/api/v1/teams")
   
   if (r.statusCode == 200 ){
      val jsonText = r.text

      val writer = new PrintWriter("teams1.json")
      writer.println(jsonText)

      writer.close()
     println("jsonText API Okey.")
     }  else {
      println("jsonText API Error.")
     }

   val r2 = requests.get("https://www.balldontlie.io/api/v1/teams?page=2")
   
   if (r.statusCode == 200 ){
      val jsonText2 = r2.text
      val writer = new PrintWriter("teams2.json")
      writer.println(jsonText2)
      writer.close()
      println("jsonText2 API Okey.")
     }    else {
      println("jsonText2 API Error.")
     }   

     println("Teams API Ended")

     // Matches API
    val team_ids: List[Int] = List(1, 13, 17, 24) // Örnek olarak takım id'lerini bir liste olarak tanımladım

    for (team_id <- team_ids) {
      println(s"$team_id") ////
      val url = s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id"
      val response = requests.get(url)

      if (response.statusCode == 200) {
        println(response.statusCode) ////
        val jsonText = response.text
        val totalPageMa = JSON.parseFull(response.text).get.asInstanceOf[Map[String, Any]]("meta").asInstanceOf[Map[String, Any]]("total_pages").asInstanceOf[Double].toInt
        println(s"Total page match = $totalPageMa")

        

        for (page <- 1 to totalPage) {
          println(s"$totalPage, su anki sayafa $page" ) ////
          val pageUrl = s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id&page=$page"
          val pageResponse = requests.get(pageUrl)

          if (pageResponse.statusCode == 200) {
            println(response.statusCode) ////
            val pageJsonText = pageResponse.text

            val writer = new PrintWriter(s"matchesID$team_id.json")
            writer.println(pageJsonText)
            writer.close()

            println(s"matchesID$team_id.json dosyası kaydedildi. Sayfa: $page")
          } else {
            println(s"Hata oluştu: ${pageResponse.statusCode}")
          }
        }
      } else {
        println(s"Hata oluştu: ${response.statusCode}")
      }
    }
        println("Matches API Ended")


    // Stats API

    val r1 = requests.get(s"https://www.balldontlie.io/api/v1/stats?seasons[]=$year&postseason=false", readTimeout = 20000, connectTimeout = 20000)
    val totalPageSt = JSON.parseFull(r1.text).get.asInstanceOf[Map[String, Any]]("meta").asInstanceOf[Map[String, Any]]("total_pages").asInstanceOf[Double].toInt
    println(s"Total page Stasts = $totalPageSt") 


        val writer = new PrintWriter("stats.json")

        for (page <- 1 to totalPage) {
        val r = requests.get(s"https://www.balldontlie.io/api/v1/stats?seasons[]=$year&postseason=false&page=$page", readTimeout = 20000, connectTimeout = 20000)

        if (r.statusCode == 200) {
                val jsonText = r.text
                writer.println(jsonText)
                println(s"Page $page - API OK")
                Thread.sleep(2000)
        } else {
                println(s"Page $page - API Error")
               }
             }
        writer.close()
        
         println("Stats API Ended")

        spark.stop()
        }
}


// ===== Buraya kadaar calisti 
















// Calisti
/// Extract Final
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import scala.util.parsing.json.JSON

object Extract {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Extract")
      .master("local[*]")
      .getOrCreate()

    val year: Int = 2021
    val totalPage: Int = 4  // sil sonra !!!!!!!!!!!!!!!!!!
    val totalPage2: Int = 50 // sill sonra !!!!!!!!!
    println(s"$year, $totalPage, Step 1 OK")


    // Teams API
    val r = requests.get("https://www.balldontlie.io/api/v1/teams")
   
   if (r.statusCode == 200 ){
      val jsonText = r.text

      val writer = new PrintWriter("teams1.json")
      writer.println(jsonText)

      writer.close()
     println("jsonText API Okey.")
     }  else {
      println("jsonText API Error.")
     }

   val r2 = requests.get("https://www.balldontlie.io/api/v1/teams?page=2")
   
   if (r.statusCode == 200 ){
      val jsonText2 = r2.text
      val writer = new PrintWriter("teams2.json")
      writer.println(jsonText2)
      writer.close()
      println("jsonText2 API Okey.")
     }    else {
      println("jsonText2 API Error.")
     }   

     println("Teams API Ended")

     // Matches API
    val team_ids: List[Int] = List(1, 13, 17, 24) // Örnek olarak takım id'lerini bir liste olarak tanımladım
    

    for (team_id <- team_ids) {
      println(s"$team_id") ////
      val url = s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id"
      val response = requests.get(url)

      if (response.statusCode == 200) {
        println(response.statusCode) ////
        val jsonText = response.text
        val totalPageMa = JSON.parseFull(response.text).get.asInstanceOf[Map[String, Any]]("meta").asInstanceOf[Map[String, Any]]("total_pages").asInstanceOf[Double].toInt
        println(s"Total page match = $totalPageMa")

        

        for (page <- 1 to totalPage) {
          println(s"$totalPage, su anki sayafa $page" ) ////
          val pageUrl = s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id&page=$page"
          val pageResponse = requests.get(pageUrl)

          if (pageResponse.statusCode == 200) {
            println(response.statusCode) ////
            val pageJsonText = pageResponse.text

            val writer = new PrintWriter(s"matchesID$team_id.json")
            writer.println(pageJsonText)
            writer.close()

            println(s"matchesID$team_id.json dosyası kaydedildi. Sayfa: $page")
          } else {
            println(s"Hata oluştu: ${pageResponse.statusCode}")
          }
        }
      } else {
        println(s"Hata oluştu: ${response.statusCode}")
      }
    }
        println("Matches API Ended")


    // Stats API

    val r1 = requests.get(s"https://www.balldontlie.io/api/v1/stats?seasons[]=$year&postseason=false", readTimeout = 20000, connectTimeout = 20000)
    val totalPageSt = JSON.parseFull(r1.text).get.asInstanceOf[Map[String, Any]]("meta").asInstanceOf[Map[String, Any]]("total_pages").asInstanceOf[Double].toInt
    println(s"Total page Stasts = $totalPageSt") 


        val writer = new PrintWriter("stats.json")

        for (page <- 1 to totalPage2) {
        val r = requests.get(s"https://www.balldontlie.io/api/v1/stats?seasons[]=$year&postseason=false&page=$page", readTimeout = 20000, connectTimeout = 20000)

        if (r.statusCode == 200) {
                val jsonText = r.text
                writer.println(jsonText)
                println(s"Page $page - API OK")
                Thread.sleep(2000)
        } else {
                println(s"Page $page - API Error")
               }
             }
        writer.close()
        
         println("Stats API Ended")

        spark.stop()
        }
}