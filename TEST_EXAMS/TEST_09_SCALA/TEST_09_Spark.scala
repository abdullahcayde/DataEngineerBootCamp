//// Extract.scala Dosyasi Step 5 ==> Read jsons
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import scala.util.parsing.json.JSON
import org.apache.spark.sql.functions._

object Extract {
  def main(args: Array[String]) {
    
      val spark = SparkSession.builder()
        .appName("Final")
        .master("local[*]")
        .getOrCreate()

      import org.apache.spark.sql.SparkSession
      import org.apache.spark.sql.functions._


      // ------------------ Teams ------------------

      val teams1Path = "/Users/macbook/Documents/Dev/spark-3.4.0-bin-hadoop3/data/teams1.json"
      val teams2Path = "/Users/macbook/Documents/Dev/spark-3.4.0-bin-hadoop3/data/teams2.json"

      val jsTeams1 = spark.read.json(teams1Path)
      val jsTeams2 = spark.read.json(teams2Path)

      println(jsTeams1.show(1))
      //println(jsTeams2.show(1))

      val dfTeams1 = jsTeams1.withColumn("data", explode(col("data"))).select("data.id", "data.abbreviation", "data.city", "data.conference", "data.division", "data.full_name", "data.name")
      val dfTeams2 = jsTeams2.withColumn("data", explode(col("data"))).select("data.id", "data.abbreviation", "data.city", "data.conference", "data.division", "data.full_name", "data.name")
      
      val dfTeamsAll = dfTeams1.union(dfTeams2)
      println(dfTeams1.count(), dfTeams2.count(), dfTeamsAll.count())

      val dfTeamsNeded = dfTeamsAll.filter(col("full_name").isin("Phoenix Suns", "Atlanta Hawks", "Milwaukee Bucks", "LA Clippers"))
      println(dfTeamsNeded.show())
      
      
      // ------------------ Matches ------------------
      val matches1Path = "/Users/macbook/Documents/Dev/spark-3.4.0-bin-hadoop3/data/matchesID1.json"
      val matches13Path = "/Users/macbook/Documents/Dev/spark-3.4.0-bin-hadoop3/data/matchesID13.json"
      val matches17Path = "/Users/macbook/Documents/Dev/spark-3.4.0-bin-hadoop3/data/matchesID17.json"
      val matches24Path = "/Users/macbook/Documents/Dev/spark-3.4.0-bin-hadoop3/data/matchesID24.json"

      val js1 = spark.read.json(matches1Path)
      val js2 = spark.read.json(matches13Path)
      val js3 = spark.read.json(matches17Path)
      val js4 = spark.read.json(matches24Path)
      val mergedJs = js1.union(js2).union(js3).union(js4)
      println(mergedJs.show(2))

      //val matchesDf01 = mergedJs.withColumn("data", explode(col("data"))).select("data.date", "data.home_team.id".as("home_team_id"), "data.home_team.full_name", "data.home_team_score", "data.visitor_team.id".as("visitor_team_id"), "data.visitor_team.full_name", "data.visitor_team_score")
      val matchesDf01 = mergedJs
                            .withColumn("data", explode(col("data")))
                            .select(
                              col("data.id").as("match_id"),
                              col("data.date"),
                              col("data.home_team.id").as("home_team_id"),
                              col("data.home_team.full_name").as("home_full_name"),
                              col("data.home_team_score"),
                              col("data.visitor_team.id").as("visitor_team_id"),
                              col("data.visitor_team.full_name").as("visitor_full_name"),
                              col("data.visitor_team_score")
                            )
      
      //println(matchesDf01.show(5))
      

      val matchesNeeded = matchesDf01.filter(col("home_team_id").isin("1", "17", "13", "24") || col("visitor_team_id").isin("1", "17", "13", "24"))
      println("matchesNeeded")
      println(matchesNeeded.show(5))

      // ------------------ Stats ------------------
      val dfstats = spark.read.json("/Users/macbook/Documents/Dev/spark-3.4.0-bin-hadoop3/data/stats_all.json")

      import org.apache.spark.sql.functions.explode

      val Stats = dfstats.withColumn("data", explode(col("data")))
        .select(
          col("data.ast"),
          col("data.blk"),
          col("data.dreb"),
          col("data.fg3_pct"),
          col("data.fg3a"),
          col("data.fg3m"),
          col("data.fg_pct"),
          col("data.fga"),
          col("data.fgm"),
          col("data.ft_pct"),
          col("data.fta"),
          col("data.ftm"),
          col("data.game.date").alias("game_date"),
          col("data.game.home_team_id").alias("home_team_id"),
          col("data.game.home_team_score").alias("home_team_score"),
          col("data.game.id").alias("game_id"),
          col("data.game.period"),
          col("data.game.postseason"),
          col("data.game.season"),
          col("data.game.status"),
          col("data.game.time"),
          col("data.game.visitor_team_id").alias("visitor_team_id"),
          col("data.game.visitor_team_score").alias("visitor_team_score"),
          col("data.id").alias("data_id"),
          col("data.min"),
          col("data.oreb"),
          col("data.pf"),
          col("data.player.first_name").alias("player_first_name"),
          col("data.player.height_feet").alias("player_height_feet"),
          col("data.player.height_inches").alias("player_height_inches"),
          col("data.player.id").alias("player_id"),
          col("data.player.last_name").alias("player_last_name"),
          col("data.player.position").alias("player_position"),
          col("data.player.team_id").alias("player_team_id"),
          col("data.player.weight_pounds").alias("player_weight_pounds"),
          col("data.pts"),
          col("data.reb"),
          col("data.stl"),
          col("data.team.abbreviation").alias("team_abbreviation"),
          col("data.team.city").alias("team_city"),
          col("data.team.conference").alias("team_conference"),
          col("data.team.division").alias("team_division"),
          col("data.team.full_name").alias("team_full_name"),
          col("data.team.id").alias("team_id"),
          col("data.team.name").alias("team_name"),
          col("data.turnover")
        )

        val StatsNeeded = Stats.filter(col("team_id").isin("1", "17", "13", "24"))
        println("StatsNeeded")
        println(StatsNeeded.select("data_id", "game_id","player_id", "player_first_name", "player_team_id", "pts","home_team_id", "home_team_score","visitor_team_id","visitor_team_score","team_id" ).show())


        // Save csv
        import org.apache.spark.sql.SaveMode
        import org.apache.spark.sql.DataFrameWriter

        StatsNeeded.write
                  .format("csv")
                  .option("header", "true") // Başlık satırını dahil etmek isterseniz "true" olarak ayarlayın
                  .mode("overwrite") // Mevcut bir dosya varsa üzerine yazmak isterseniz "overwrite" olarak ayarlayın
                  .save("/home/ubuntu/finalScala/final.csv")










         spark.stop()
        }
}




//// Extract.scala Dosyasi Step 4 ==> Stats cekilecek

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

    val r1 = requests.get(s"https://www.balldontlie.io/api/v1/stats?seasons[]=$year&postseason=false", readTimeout = 20000, connectTimeout = 20000)
    val totalPage = JSON.parseFull(r1.text).get.asInstanceOf[Map[String, Any]]("meta").asInstanceOf[Map[String, Any]]("total_pages").asInstanceOf[Double].toInt
    println("Total page Stasts")  // Sil sonra 
    println(totalPage)

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
      .appName("Extract")
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
      .appName("Extract")
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
      .appName("Extract")
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
      .appName("Extract")
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
      .appName("Extract")
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
      .appName("Extract")
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
      .appName("Extract")
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



//
Spark bilginizi pekiştirmek için aşağıdaki alıştırmayı yapacaksınız. Amaç, Scala ile NBA hakkında veri veren bu API'yi kullanarak basit bir ETL yapmak ve ardından Spark ile toplanan verileri CSV dosyalarına sahip olacak şekilde işlemek olacaktır. API istemek için anahtar gerekmez.

Veri çıkarma
HTTP API'leri istemek için, esas olarak python'un istek kitaplığından geçiyoruz. Python'un ana özelliklerini içeren Scala'da bir karşılığı vardır.

API aşağıdaki yollara sahiptir:
     oyuncular
     takımlar
     oyunlar
     istatistikler

     Aşağıdaki takımların 2021-2022 sezonu maçlarını toplayın :
    Phoenix Suns
    Atlanta Hawks
    Los Angeles Clippers
    Milwaukee Bucks

     Ardından, seçili maçların istatistiklerini yukarı akışta toplayın.

Lütfen unutmayın, bir ekibin sonucunda tüm veriler görüntülenmez, her şeyi kurtarmak için sayfalarda yineleme yapılması gerekecektir.
Bu aşamanın sonunda birkaç json dosyasına sahip olmanız gerekecek.

Veri dönüşümü
Amaç, toplanan iki "veri türünü" birleştirmek ve bunları csv biçiminde depolamaktır.

     Maç verilerinde:
Takımın attığı puan sayısı, maç kimliği, takımın adı ve kimliği ile bir DataFrame yapmanız gerekecek.

     İstatistik verilerinde:
Önceki adımda olduğu gibi, yalnızca belirtilen ekipleri kurtarmanız gerekecek.
Bundan sonra, işaretlenen puanlar, yapılan paslar vb. gibi oyuncuların istatistiklerini seçmek gerekecektir. 
Son olarak, çizgileri takıma ve maç kimliğine göre gruplandırın.

     Elde edilen her iki DataFrame'i birleştirin ve DataFrame finalini bir csv dosyasına kaydedin.
     //


// To provide more context, here are the meanings of the columns in an NBA dataset:
    gp: Games played
    min: Minutes played
    fgm: Field goals made
    fga: Field goals attempted
    fg_pct: Field goal percentage
    fg3m: Three-point field goals made
    fg3a: Three-point field goals attempted
    fg3_pct: Three-point field goal percentage
    ftm: Free throws made
    fta: Free throws attempted
    ft_pct: Free throw percentage
    oreb: Offensive rebounds
    dreb: Defensive rebounds
    reb: Total rebounds
    ast: Assists
    stl: Steals
    blk: Blocks
    tov: Turnovers
    pf: Personal fouls
    pts: Total points
    ast_tov: Assist-to-turnover ratio
    stl_tov: Steal-to-turnover ratio
    efg_pct: Effective field goal percentage
    ts_pct: True shooting percentage

    
    ast: Asist (assist) sayısı
    blk: Blok (block) sayısı
    dreb: Savunma ribaundu (defensive rebound) sayısı
    fg3_pct: Üçlük atış yüzdesi
    fg3a: Üçlük atışı deneme (attempt) sayısı
    fg3m: İsabetli üçlük atışı (made) sayısı
    fg_pct: İki nokta atışı yüzdesi
    fga: İki nokta atışı deneme sayısı
    fgm: İsabetli iki nokta atışı sayısı
    ft_pct: Serbest atış yüzdesi
    fta: Serbest atış deneme sayısı
    ftm: İsabetli serbest atış sayısı
    game: Maç bilgilerini içeren struct yapısı
    id: Veri öğesinin kimlik numarası
    min: Oyunda geçirilen süre (dakika)
    oreb: Hücum ribaundu (offensive rebound) sayısı
    pf: Faul (personal foul) sayısı
    player: Oyuncu bilgilerini içeren struct yapısı
    pts: Toplam sayı (points) sayısı
    reb: Toplam ribaund sayısı
    stl: Top çalma (steal) sayısı
    team: Takım bilgilerini içeren struct yapısı
    turnover: Top kaybı (turnover) sayısı




 // Notlar 

// Json okumak ve tek tek kolonlara ayirmak 
val jsPath = "/home/ubuntu/finalScala/teams1.json"
val js01 = spark.read.json(dfPath)
println(js01.show(1))
val df01 = js01.withColumn("data", explode(col("data"))).select("data.id", "data.abbreviation", "data.city", "data.conference")


// Json dosya birlestirmek (union)
val js1 = spark.read.json(js1Path)
val js2 = spark.read.json(js2Path)
val js3 = spark.read.json(js3Path)

val mergedJs = js1.union(js2).union(js3)
println(mergedJs.show(2))

// csv veya json olarak kaydetmek (tek parca olmasi icin coalasce(1))
import java.io.PrintWriter

df.coalesce(1).write.csv("/home/ubuntu/finalScala/df.csv")
df.coalesce(1).write.json("/home/ubuntu/finalScala/df.json")

// ---- csv iki parca halinde kayit edildiyse birlestirmek icin
cat part-* > merged.csv   // butun part ile baslyan dosyalari merge.csv de birlestirir.


// Except , .collect.toList
val exceptList = dfm.select("match_id").except(dfs.select("game_id")).collect.toList
exceptList.length
exceptList

val exceptList = matches.select("match_id").except(statsFinal.select("match_id")).collect.toList


// groupBy , orderBy, count
df.select("game_id").groupBy("game_id").count().orderBy("game_id").show()
// order by DESC
df.select("game_id").groupBy("game_id").count().orderBy($"game_id".desc).show()

// distinct
df.select("game_id").distinct().count()

// Filter , isin 
df.select("game_id").filter(col("game_id").isin("473663", "473607", "473505")).distinct().show()
//yada
val values = Array("473663", "473607", "473505")
df.select("game_id").filter(col("game_id").isin(values:_*)).distinct().show()

// join 2 data frame ayni kolonlar = "match_id", "team_id"
val mergeDf = matches.join(st, Seq("match_id", "team_id"))

// Agg , groupBy
dfs.groupBy("team_id")
  .agg(
    sum("home_team_score"), 
    max("home_team_score"),
    count("*").as("Nb_Rows"), 
    avg("pts").as("pst_avg")
  ).show()


dfs.groupBy("game_id", "team_id")
  .agg(
    sum("home_team_score"), 
    max("home_team_score"),
    count("*").as("Nb_Rows"), 
    avg("pts").as("pst_avg")
  ).orderBy("game_id").show()


  
    statsFinal.groupBy("team_id").agg(sum("score"), max("score"), count("*").as("Nb_Rows"), avg("pts").as("pst_avg")).show()

      // ------------------ Local ------------------
      //Import Libararies
      import org.apache.spark.sql.SparkSession
      import org.apache.spark.sql.functions._
      import org.apache.spark.sql.functions.explode


      // ------------------ Teams Data ------------------
      // Read Teams Data as Json
      val path = "/Users/macbook/Documents/Dev/spark-3.4.0-bin-hadoop3/data"
      val teams1Path = s"$path/teams1.json"
      val teams2Path = s"$path/teams2.json"

      val jsTeams1 = spark.read.json(teams1Path)
      val jsTeams2 = spark.read.json(teams2Path)

      // Convert to Dataframe
      val dfTeams1 = jsTeams1.withColumn("data", explode(col("data"))).select("data.id", "data.abbreviation", "data.city", "data.conference", "data.division", "data.full_name", "data.name")
      val dfTeams2 = jsTeams2.withColumn("data", explode(col("data"))).select("data.id", "data.abbreviation", "data.city", "data.conference", "data.division", "data.full_name", "data.name")
      
      val dfTeamsAll = dfTeams1.union(dfTeams2)
      println(dfTeams1.count(), dfTeams2.count(), dfTeamsAll.count())

      val dfTeamsNeded = dfTeamsAll.filter(col("full_name").isin("Phoenix Suns", "Atlanta Hawks", "Milwaukee Bucks", "LA Clippers"))
      println("dfTeamsNeded")
      dfTeamsNeded.show()
      
      
      // ------------------ Matches Data ------------------
      // Read Matches Data as Json
      val js1 = spark.read.json(s"$path/matchesID1.json")
      val js2 = spark.read.json(s"$path/matchesID13.json")
      val js3 = spark.read.json(s"$path/matchesID17.json")
      val js4 = spark.read.json(s"$path/matchesID24.json")
      val mergedJs = js1.union(js2).union(js3).union(js4)

      // Convert to Dataframe
      val matchesDf = mergedJs
                            .withColumn("data", explode(col("data")))
                            .select(
                              col("data.id").as("match_id"),
                              col("data.date"),
                              col("data.home_team.id").as("home_team_id"),
                              col("data.home_team_score"),
                              col("data.visitor_team.id").as("visitor_team_id"),
                              col("data.visitor_team_score")
                            )
      

      // Filter Date year 2021      
      val matchesDfF2021 = matchesDf.filter(col("date").contains("2021")).drop(col("date"))
      
      // Filter Team ids (1, 13, 17,24)
      val matchesDfF2021FTeamId = matchesDfF2021.filter(col("home_team_id").isin("1", "17", "13", "24") || col("visitor_team_id").isin("1", "17", "13", "24"))
      
      // Convert to Integer
      val matchesDfF2021FTeamIdCo = matchesDfF2021FTeamId.columns.foldLeft(matchesDfF2021FTeamId) { (tempDF, colName) => tempDF.withColumn(colName, col(colName).cast("integer"))}
      println("matchesDfF2021FTeamIdCo")
      println(matchesDfF2021FTeamIdCo.show(5))

      // Union Dataframe
      val matchesDfF2021FTeamIdCoUnion = matchesDfF2021FTeamIdCo.select(
                            col("match_id"),
                            col("home_team_id").alias("team_id"),
                            col("home_team_score").alias("score"),
                            lit("home").alias("where")
                          ).union(
                            matchesDfF2021FTeamIdCo.select(
                              col("match_id"),
                              col("visitor_team_id").alias("team_id"),
                              col("visitor_team_score").alias("score"),
                              lit("visitor").alias("where")
                            )
                          )
      println("matchesDfF2021FTeamIdCoUnion")
      matchesDfF2021FTeamIdCoUnion.show(5)     

      // Final Match Data (matches)
      val matches = matchesDfF2021FTeamIdCoUnion.join(dfTeamsNeded.select("id", "full_name"), matchesDfF2021FTeamIdCoUnion("team_id") === dfTeamsNeded("id")).drop("id","where")
      println("matches")
      println(matches.show(5))



      // ------------------ Stats ------------------
      // Reads Statistics data as Json
      val dfstats = spark.read.json(s"$path/stats_all.json")

      // Convert to Dataframe
      val Stats = dfstats.withColumn("data", explode(col("data")))
        .select(
          col("data.ast"), col("data.blk"), col("data.dreb"), col("data.fg3_pct"), col("data.fg3a"),col("data.fg3m"), col("data.fg_pct"), 
          col("data.fga"), col("data.fgm"), col("data.ft_pct"),
          col("data.fta"), col("data.ftm"),
          col("data.game.date").alias("game_date"),
          col("data.game.home_team_id").alias("home_team_id"),
          col("data.game.home_team_score").alias("home_team_score"),
          col("data.game.id").alias("match_id"),
          col("data.game.period"),
          col("data.game.postseason"),
          col("data.game.season"),
          col("data.game.status"),
          col("data.game.time"),
          col("data.game.visitor_team_id").alias("visitor_team_id"),
          col("data.game.visitor_team_score").alias("visitor_team_score"),
          col("data.id").alias("data_id"),
          col("data.min"), col("data.oreb"), col("data.pf"),
          col("data.player.first_name").alias("player_first_name"),
          col("data.player.height_feet").alias("player_height_feet"),
          col("data.player.height_inches").alias("player_height_inches"),
          col("data.player.id").alias("player_id"),
          col("data.player.last_name").alias("player_last_name"),
          col("data.player.position").alias("player_position"),
          col("data.player.team_id").alias("player_team_id"),
          col("data.player.weight_pounds").alias("player_weight_pounds"),
          col("data.pts"), col("data.reb"), col("data.stl"),
          col("data.team.abbreviation").alias("team_abbreviation"),
          col("data.team.city").alias("team_city"),
          col("data.team.conference").alias("team_conference"),
          col("data.team.division").alias("team_division"),
          col("data.team.full_name").alias("team_full_name"),
          col("data.team.id").alias("team_ID_"),
          col("data.team.name").alias("team_name"),
          col("data.turnover")
        )
        // Drop Duplicates
        val StatsDropDup = Stats.dropDuplicates()

        // Filter Teams with Id and select only date 2021
        val StatsF2021 = StatsDropDup.filter(col("game_date").contains("2021"))
        // onceki hali sil ==> val dfs01 = Stats.filter(col("team_ID_").isin("1", "17", "13", "24")).filter(col("game_date").contains("2021"))
        // Filter  match_id with Match DataFrame and Stats DataFrame
        val listMatch = dfm.select("match_id").as[String].collect.toList
        val StatsF2021FGameId = StatsF2021.filter(col("match_id").isin(listMatch: _*))
        
        // Convert Column Types
        val integerColumns = Seq(
                                "ast", "blk", "dreb", "fg3a", "fg3m", "fga", "fgm", "fta", "ftm", "home_team_id", "home_team_score", "match_id", "period", "season",
                                "visitor_team_id", "visitor_team_score", "data_id", "oreb", "pf","player_height_feet", "player_height_inches", "player_id","player_team_id",
                                "player_weight_pounds", "pts", "reb", "stl", "team_ID_", "turnover"
                              )

        val doubleColumns = Seq("fg3_pct", "fg_pct", "ft_pct")

        val stringColumns = Seq(
                                "game_date", "status", "time", "min", "player_first_name", "player_last_name", "player_position", "team_abbreviation",
                                "team_city", "team_conference", "team_division","team_full_name", "team_name"
                              )

        val dfs = StatsF2021FGameId.select(
                                  integerColumns.map(col(_).cast("integer")) ++
                                  doubleColumns.map(col(_).cast("double")) ++
                                  stringColumns.map(col(_).cast("string")): _*
                                    )

        // Print                                     
        println("dfs")
        println(dfs.select("data_id","team_ID_", "match_id","player_id", "player_first_name", "player_team_id", "pts","home_team_id", "home_team_score","visitor_team_id","visitor_team_score" ).show(5))

        // Stats join home and visitor
        val df_new = dfs.select(
          col("match_id"),
          col("home_team_id").alias("team_id"),
          col("home_team_score").alias("score"),
          lit("home").alias("where")
        ).union(
          dfs.select(
            col("match_id"),
            col("visitor_team_id").alias("team_id"),
            col("visitor_team_score").alias("score"),
            lit("visitor").alias("where")
          )
        )

        val stats = dfs.join(df_new, dfs("match_id") === df_new("match_id")).drop("home_team_id", "home_team_score","visitor_team_id","visitor_team_score").drop(df_new("match_id"))
        val statsDropDup = stats.dropDuplicates()
        println("statsFinal")
        val statsFinal = statsDropDup.filter(col("team_id").isin(1,13,17,24))
        statsFinal.select("team_ID_", "match_id","player_id", "player_first_name", "player_team_id","team_id", "score").show(3)
        //statsFinal.select( "data_id","match_id","team_id", "score").show()
        //statsFinal.select( "team_id", "match_id", "score", "player_id", "player_first_name", "player_team_id").show()
        val st = statsFinal.select("match_id","team_id", "score", "player_id",  "player_first_name", "player_last_name", "pts", "reb", "stl", "ast", "blk", "dreb", "fg3a", "fg3m", "fga", "fgm", "fta", "ftm", "fg3_pct", "fg_pct", "ft_pct")
        st.show(3)


        // Final Data
        println("FinalData")
        val FinalData = matches.join(st, Seq("match_id", "team_id"))
        FinalData.show(5)

        // Data Analysis
        statsFinal.groupBy("team_id").agg(sum("score"), max("score"), count("*").as("Nb_Rows"), avg("pts").as("pst_avg")).show()
        statsFinal.groupBy("team_id", "match_id").agg(sum("ast"), min("score").as("score"), count("*").as("Nb_Rows"), avg("pts").as("pts_avg")).orderBy("match_id").show()
        statsFinal.groupBy("player_id").agg( count("*").as("Nb_Rows"),sum("pts"), avg("pts").as("pts_avg"), sum("stl"), sum("reb")).orderBy($"pts_avg".desc).show()


