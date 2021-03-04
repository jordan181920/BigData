import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, explode, second}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.Predef.any2stringadd

object JsonParser {

  val conf = new SparkConf().setMaster("local[2]").setAppName("TabellaQuery")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)
  val input = "D:\\BigData\\ProgettoBigData\\Firs500Rows.json"

  import hiveContext.implicits._
  def main(args: Array[String]){

    //parso il file
    val df_event = sqlContext.read.json(input)
    val new_df_event = df_event.withColumnRenamed("public", "publicField")
    //creo dataSet
    val ds_event = new_df_event.as[Event]
    //creo rdd
    val rdd_event = ds_event.rdd


    //TODO:1.1)trovare i singoli actor
    //DF
    val df_actor = new_df_event.select("actor").distinct()
    df_actor.show()
    //RDD
    val rdd_actor = rdd_event.map(x => x.actor).distinct()
    rdd_actor.take(10).foreach(println)

    //TODO:1.2)trovare i singoli author dentro commit
    ////DF = cambio da event a commit
    val payload_df = df_event.select("payload.*")
    val commits_df = payload_df.select(explode(col("commits"))).select("col.*")
    val author_df = commits_df.select("author")
    author_df.show()
    //RDD
    val rdd_commit = commits_df.as[Commit].rdd
    val rdd_author = rdd_commit.map(x => x.author).distinct()
    rdd_author.take(10).foreach(println)

    //TODO:1.3)trovare i singoli repo
    //DF
    val df_repo = new_df_event.select("repo").distinct()
    df_repo.show()
    //RDD
    val rdd_repo = rdd_event.map(x => x.repo).distinct()
    rdd_repo.take(10).foreach(println)

    //TODO:1.4)trovare i vari tipi di evento type
    //DF
    val df_type = new_df_event.select("`type`").distinct()
    df_type.show()
    //RDD
    val rdd_type = rdd_event.map(x => x.`type`).distinct()
    rdd_type.take(10).foreach(println)

    //TODO:1.5)contare il numero di actor
    //DF
    val df_actor = new_df_event.select("actor").distinct().count()
    println(df_actor)
    //RDD
    val rdd_ac = rdd_event.map(x => x.actor).distinct().count()
    println(rdd_ac)

    //TODO:1.6)contare il numero di repo
    //DF
    val df_repo = new_df_event.select("repo").distinct().count()
    println(df_repo)
    //RDD
    val rdd_rep = rdd_event.map(x => x.repo).distinct().count()
    println(rdd_rep)

    //TODO:2.1)contare numero event per ogni actor
    //DF
    val df_nEvent = new_df_event.select("actor").count()
    println(df_nEvent)
    //RDD
    val rdd_a = rdd_event.map(x => x.actor).count()
    println(rdd_a)


    //14.5
    //df
    val second_df = new_df.withColumn("second", second($"created_at"))
    val second_count_df = second_df.select($"second", count($"*").over(Window.partitionBy($"second")) as "count")
    val second_max_df = second_count_df.agg(max("count"))
    val second_min_df = second_count_df.agg(min("count"))
    second_max_df.show()
    second_min_df.show()
    //RDD
    val second_RDD = dataRDD.map(x=>(x.created_at.getTime, 1L)).reduceByKey((count1, count2)=> count1 + count2)
    val max_second_RDD = second_RDD.map(x => x._2).max()
    val min_second_RDD = second_RDD.map(x => x._2).min()
    println("Max conteggio RDD " + max_second_RDD)
    println("Min conteggio RDD " + min_second_RDD)

    //trovare il max/min numero di event per actor
    //14.6
    //df
    val count_df = new_df.select(col("actor"), count($"*").over(Window.partitionBy("actor")) as "newCount")
    val max_event_df= count_df.agg(max("newCount"))
    val min_event_df= count_df.agg(min("newCount"))
    max_event_df.show()
    min_event_df.show()
    //RDD
    val count_RDD = dataRDD.map(x=>(x.actor, x)).aggregateByKey(0)((count, actor)=> count + 1, (count1, count2)=> count1 + count2 )
    val max_count_RDD = count_RDD.map(x=>x._2).max()
    val min_count_RDD = count_RDD.map(x=>x._2).min()
    println("Max conteggio RDD " + max_count_RDD)
    println("Min conteggio RDD " + min_count_RDD)

    //trovare il max/min numero di event per repo
    //14.7
    //df
    val count_repo_df = new_df.select(col("repo"), count($"*").over(Window.partitionBy("repo")) as "count")
    val max_repo_df = count_repo_df.agg(max("count"))
    val min_repo_df = count_repo_df.agg(min("count"))
    max_repo_df.show()
    min_repo_df.show()
    //RDD
    val count_repo_RDD = dataRDD.map(x=>(x.repo, x)).aggregateByKey(0)((count, repo)=> count + 1, (count1, count2)=> count1 + count2 )
    val max_repo_RDD = count_repo_RDD.map(x=>x._2).max()
    val min_repo_RDD = count_repo_RDD.map(x=>x._2).min()
    println("Max conteggio RDD " + max_repo_RDD)
    println("Min conteggio RDD " + min_repo_RDD)

    //trovare il max/min numero di event per secondo per actor
    //14.8
    //df
    val second_actor_df = new_df.withColumn("second", second($"created_at"))
    val count_esa_df = second_actor_df.select(col("actor"), col("second"), count($"*").over(Window.partitionBy("actor", "second")) as "count")
    val max_event_second_actor = count_esa_df.agg(max("count"))
    val min_event_second_actor = count_esa_df.agg(min("count"))
    max_event_second_actor.show()
    min_event_second_actor.show()
    //RDD
    val count_esa_RDD = dataRDD.map(x=>((x.actor,x.created_at.getTime), 1L)).reduceByKey((count1, count2) => count1 + count2)
    val max_esa_RDD = count_esa_RDD.map(x=>x._2).max()
    val min_esa_RDD = count_esa_RDD.map(x=>x._2).min()
    println("Max conteggio RDD " + max_esa_RDD)
    println("Min conteggio RDD " + min_esa_RDD)

    //trovare il max/min numero di event per secondo per repo
    //14.9
    //df
    val second_repo_df = new_df.withColumn("second", second($"created_at"))
    val count_esr_df = second_repo_df.select(col("repo"), col("second"), count($"*").over(Window.partitionBy("repo", "second")) as "count")
    val max_event_second_repo = count_esr_df.agg(max("count"))
    val min_event_second_repo = count_esr_df.agg(min("count"))
    max_event_second_repo.show()
    min_event_second_repo.show()
    //RDD
    val count_esr_RDD = dataRDD.map(x=>((x.repo,x.created_at.getTime), 1L)).reduceByKey((count1, count2) => count1 + count2)
    val max_esr_RDD = count_esr_RDD.map(x=>x._2).max()
    val min_esr_RDD = count_esr_RDD.map(x=>x._2).min()
    println("Max conteggio RDD " + max_esr_RDD)
    println("Min conteggio RDD " + min_esr_RDD)
  }
}