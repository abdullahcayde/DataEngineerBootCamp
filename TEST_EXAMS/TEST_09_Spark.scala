//// Extract.scala Dosyasi Step 4 ==> Stats cekilecek

import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import scala.util.parsing.json.JSON

object Extract {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Final")
      .master("local[*]")
      .getOrCreate()


    val year: Int = 2021

    val r1 = requests.get(s"https://www.balldontlie.io/api/v1/stats?seasons[]=$year&postseason=false", readTimeout = 20000, connectTimeout = 20000)
    val totalPage = JSON.parseFull(r1.text).get.asInstanceOf[Map[String, Any]]("meta").asInstanceOf[Map[String, Any]]("total_pages").asInstanceOf[Double].toInt


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
        
        spark.stop()
        }
}




//// Extract.scala Dosyasi Step 3.5 ==> id_team Listesi eklendi herbir Id icin json dosyasi kayit edildi
// ve Her ID icin total_pages Dongusu olusturuldu. (Caslisti , readTimeout = 20000, connectTimeout = 20000) ekleyince )

import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import scala.util.parsing.json.JSON

object Extract {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Final")
      .master("local[*]")
      .getOrCreate()

    val teamId: Int = 1
    val teamIdList: List[Int] = List(1, 13, 17, 24)
    val year: Int = 2021

    val r1 = requests.get(s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$teamId", readTimeout = 20000, connectTimeout = 20000)
    val totalPage = JSON.parseFull(r1.text).get.asInstanceOf[Map[String, Any]]("meta").asInstanceOf[Map[String, Any]]("total_pages").asInstanceOf[Double].toInt


    for (team_id <- teamIdList) {
        val writer = new PrintWriter(s"matchesID$team_id.json")

        for (page <- 1 to totalPage) {
        val r = requests.get(s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id&page=$page")

        if (r.statusCode == 200) {
                val jsonText = r.text
                writer.println(jsonText)
                println(s"Page $team_id , $page - API OK")
        } else {
                println(s"Page $team_id , $page - API Error")
               }
             }
        writer.close()
        }
        
        spark.stop()
        }
}








//// Extract.scala Dosyasi Step 3.4 ==> id_team Listesi eklendi herbir Id icin json dosyasi kayit edildi. (Calismiyor )
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import scala.util.parsing.json.JSON

object Extract {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Final")
      .master("local[*]")
      .getOrCreate()

    val team_ids: List[Int] = List(1, 13, 17, 24) // Örnek olarak takım id'lerini bir liste olarak tanımladım
    val year: Int = 2021

    for (team_id <- team_ids) {
      println(s"$team_id") ////
      val url = s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id"
      val response = requests.get(url)

      if (response.statusCode == 200) {
        println(response.statusCode) ////
        val jsonText = response.text
        val totalPage = JSON.parseFull(response.text).get.asInstanceOf[Map[String, Any]]("meta").asInstanceOf[Map[String, Any]]("total_pages").asInstanceOf[Double].toInt

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

    spark.stop()
  }
}

[error] [error] [error][error][error][error][error][error][error][error][error][error][error]
:: retrieving :: org.apache.spark#spark-submit-parent-558abae7-fb54-44fc-a879-8208fe40fb9a
	confs: [default]
	0 artifacts copied, 2 already retrieved (0kB/8ms)
Exception in thread "main" requests.TimeoutException: 
        Request to https://www.balldontlie.io/api/v1/games?seasons[]=2021&team_ids[]=1&page=1 timed out. (readTimeout: 10000, connectTimout: 10000)
[error][error][error][error][error][error][error][error][error][error][error][error][error][error]


//// Extract.scala Dosyasi Step 3.3  ===> Dongu olusturmak En Sonunda Calisti
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import scala.util.parsing.json.JSON

object Extract {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Final")
      .master("local[*]")
      .getOrCreate()

    val team_id: Int = 1
    val year: Int = 2021

    val r1 = requests.get(s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id")
    val totalPage = JSON.parseFull(r1.text).get.asInstanceOf[Map[String, Any]]("meta").asInstanceOf[Map[String, Any]]("total_pages").asInstanceOf[Double].toInt

    val writer = new PrintWriter("matchesId1.json")

    for (page <- 1 to totalPage) {
      val r = requests.get(s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id&page=$page")

      if (r.statusCode == 200) {
        val jsonText = r.text
        writer.println(jsonText)
        println(s"Page $page - API OK")
      } else {
        println(s"Page $page - API Error")
      }
    }

    writer.close()
    spark.stop()
  }
}


//// Extract.scala Dosyasi Step 3.2  ===> Dongu olusturmak (Asagidakilerin hicbiri calismadi !!!!)
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
//import play.api.libs.json.Json
import scala.util.parsing.json.JSON

object Extract {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Final")
      .master("local[*]")
      .getOrCreate()

    val team_id: Int = 1
    val year: Int = 2021

    val r1 = requests.get(s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id")
    //val totalPage = r1.json()["meta"]["total_pages"].intValue()
    //val totalPage = r1.json.as[Map[String, Any]]("meta")("total_pages").asInstanceOf[Double].toInt
    //val totalPage = r1.json.as[Map[String, Any]]("meta").asInstanceOf[Map[String, Any]]("total_pages").asInstanceOf[Double].toInt
    //val totalPage = (Json.parse(r1.text) \ "meta" \ "total_pages").as[Double].toInt


    val writer = new PrintWriter("matchesId1.json")

    for (page <- 1 to totalPage) {
      val r = requests.get(s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id&page=$page")

      if (r.statusCode == 200) {
        val jsonText = r.text
        writer.println(jsonText)
        println(s"Page $page - API OK")
      } else {
        println(s"Page $page - API Error")
      }
    }

    writer.close()
    spark.stop()
  }
}

[error] /home/ubuntu/finalScala/src/main/scala/Extract.scala:15:31: identifier expected but string literal found.
[error]     val totalPage = r1.json()["meta"]["total_pages"].intValue()
[error]                               ^
[error] /home/ubuntu/finalScala/src/main/scala/Extract.scala:17:1: ']' expected.
[error]     val writer = new PrintWriter("matchesId1.json")
[error] ^
[error] two errors found





//// Extract.scala Dosyasi Step 3  ===> Dongu olusturmak
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter

object Extract {
  def main(args: Array[String]) {
   val spark = SparkSession.builder()
      .appName("Final")
      .master("local[*]")
      .getOrCreate()
  
  val team_id: Int = 1
  val year: Int = 2021
  val totalPage: Int = 10

  val writer = new PrintWriter("matchesId1.json")

    for (page <- 1 to totalPage) {
      val r = requests.get(s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id&page=$page")

      if (r.statusCode == 200) {
        val jsonText = r.text
        writer.println(jsonText)
        println(s"Page $page - API OK")
      } else {
        println(s"Page $page - API Error")
      }
    }

    writer.close()
    spark.stop()
  }
}


// Extract.scala Dosyasi Step 2 == > matches1.json eklendi
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter

object Extract {
  def main(args: Array[String]) {
   val spark = SparkSession.builder()
      .appName("Final")
      .master("local[*]")
      .getOrCreate()

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
  
  val team_id : Int = 1
  val year : Int = 2021
  val r3 = requests.get(s"https://www.balldontlie.io/api/v1/games?seasons[]=$year&team_ids[]=$team_id")
  
  if (r3.statusCode == 200 ){
      val jsonText3 = r3.text

      val writer = new PrintWriter("matchesId1.json")
      writer.println(jsonText3)

      writer.close()
     println(s"JsonText3 API Okey.")
}   else {
      println("jsonText3 API Error.")
    }

    spark.stop()
  }
}



//// Extract.scala Dosyasi Step 1 
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter

object Extract {
  def main(args: Array[String]) {
   val spark = SparkSession.builder()
      .appName("Final")
      .master("local[*]")
      .getOrCreate()

   val r = requests.get("https://www.balldontlie.io/api/v1/teams")
   
   if (r.statusCode == 200 ){
      val jsonText = r.text

      val writer = new PrintWriter("teams1.json")
      writer.println(jsonText)

      writer.close()
     println("API Okey.")
}
   val r2 = requests.get("https://www.balldontlie.io/api/v1/teams?page=2")
   
   if (r.statusCode == 200 ){
      val jsonText2 = r2.text
      val writer = new PrintWriter("teams2.json")
      writer.println(jsonText2)
      writer.close()
      println("API Okey.")
}    else {
      println("API Error.")
    }
    spark.stop()
  }
}