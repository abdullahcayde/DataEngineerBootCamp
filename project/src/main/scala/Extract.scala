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
