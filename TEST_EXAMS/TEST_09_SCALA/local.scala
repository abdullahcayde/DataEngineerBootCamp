/// Transform Final Calisti
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
        val listMatch = StatsF2021.select("match_id").as[String].collect.toList
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

        


