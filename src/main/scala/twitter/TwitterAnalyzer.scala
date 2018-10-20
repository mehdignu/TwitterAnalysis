package twitter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.time.OffsetDateTime

class TwitterAnalyzer (tData:RDD[Row]){
  
  /*
   * Write a function that counts the number of tweets using the german lanuage
   */
  def getGermanTweetsCount:Long= ???
  
  /*
   * Write a function that extracts the texts of all german tweets
   */
  def getGermanTweetTexts:Array[String]= ???
  
    /*
   * Write a function that counts the number of german tweets that users created  
   */
  def numberOfGermanTweetsPerUser:Array[(String,Int)]= ???
  
   /*
   * Write a function that finds the top ten hashtags by extracting them from their texts  
   */
  def getTopTenHashtags:List[(String, Int)]= ???  

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

