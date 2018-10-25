package twitter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.time.OffsetDateTime

class TwitterAnalyzer (tData:RDD[Row]){
  
  /*
   * Write a function that counts the number of tweets using the german lanuage
   */
  def getGermanTweetsCount:Long = {
    val data = tData
    data.filter(x => x(3) == "de").count()
  }



  
  /*
   * Write a function that extracts the texts of all german tweets
   */
  def getGermanTweetTexts:Array[String]= {
    val data = tData
    data.filter(x => x(3) == "de").map(x => x(2).toString).collect().toArray
  }
  
    /*
   * Write a function that counts the number of german tweets that users created  
   */
  def numberOfGermanTweetsPerUser:Array[(String,Int)]= {
    val data = tData
    data.filter(x => x(3) == "de").groupBy(x => x(1)).map(x => (x._1.toString, x._2.count(c => true))).collect().toArray
  }
  
   /*
   * Write a function that finds the top ten hashtags by extracting them from their texts  
   */
  def getTopTenHashtags:List[(String, Int)]= {

    val f = TwitterAnalyzer.getHashtags(_: String)
    val data = tData

    data.map(x => f(x(2).toString)).flatMap(x => x).groupBy(f => f).map(x => (x._1, x._2.size)).sortBy(x => x._2, ascending = false).collect().toList

  }

}

object TwitterAnalyzer{
    

  def getHashtags(text:String):List[String]={
    

    if (text.isEmpty || text.length==1) List()
    else if (text.head=='#') {
      val tag= text.takeWhile(x=> (x!=' '))
      val rest=text.drop(tag.length)
      tag::getHashtags(rest)
    }
       else getHashtags(text.tail)
  }
}

