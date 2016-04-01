import java.io._
import java.util.Properties
import java.util.regex.{Matcher, Pattern}

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Text
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.atilika.kuromoji.{Token, Tokenizer}
//import twitter4j._
//import twitter4j.conf._
//import scala.collection.JavaConversions._
import com.opencsv.CSVWriter

/**
  * Created by AKB428 on 2015/06/05.
  *
  * ref:https://github.com/AKB428/inazuma/blob/master/src/main/scala/inazumaTwitter.scala
  * ref:http://www.intellilink.co.jp/article/column/bigdata-kk01.html
  */
object MikasaGeneralNotKafka {
  /**
    *
    * @param args args(0)=application.properties path (default config/application.properties)
    */
  def main(args: Array[String]): Unit = {
    //--- Kafka Client Init End
    var configFileName = "config/application.properties"


    if (args.length == 1) {
      configFileName = args(0)
    }

    // Load Application Config
    val inStream = new FileInputStream(configFileName)
    val appProperties = new Properties()
    appProperties.load(new InputStreamReader(inStream, "UTF-8"))

    val dictFilePath = appProperties.getProperty("kuromoji.dict_path")
    val takeRankNum = appProperties.getProperty("take_rank_num").toInt

    // TODO @AKB428 サンプルコードがなぜかシステム設定になってるのでプロセス固有に設定できるように直せたら直す
    System.setProperty("twitter4j.oauth.consumerKey", appProperties.getProperty("twitter.consumer_key"))
    System.setProperty("twitter4j.oauth.consumerSecret", appProperties.getProperty("twitter.consumer_secret"))
    System.setProperty("twitter4j.oauth.accessToken", appProperties.getProperty("twitter.access_token"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", appProperties.getProperty("twitter.access_token_secret"))


    // https://spark.apache.org/docs/latest/quick-start.html
    val conf = new SparkConf().setAppName("Mikasa Online Layer")
    conf.setMaster("local[*]")
    //val sc = new SparkContext(conf)

    // Spark Streaming本体（Spark Streaming Context）の定義
    val ssc = new StreamingContext(conf, Seconds(60)) // スライド幅60秒

    // 設定ファイルより検索ワードを設定
    val searchWordList = appProperties.getProperty("twitter.searchKeyword").split(",")

    // debug
    // println(searchWordList(0))

    //都道府県
    val prefectures = Array("北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県", "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県",
      "東京都", "神奈川県", "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県", "三重県", "滋賀県", "京都府",
      "大阪府", "兵庫県", "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県", "徳島県", "香川県", "愛媛県", "高知県", "福岡県",
      "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県")

    //val stream = TwitterUtils.createStream(ssc, None, searchWordList)
    //ワードサーチなし
    val stream = TwitterUtils.createStream(ssc, None)
   // Twitterから取得したツイートを処理する
    //val tweetGeoStream = stream.filter()
    val  tweetStream = stream.flatMap(status => {

      //      val tokenizer : Tokenizer = CustomTwitterTokenizer.builder().build()  // kuromojiの分析器
      val features : scala.collection.mutable.ArrayBuffer[String] = new collection.mutable.ArrayBuffer[String]() //解析結果を保持するための入れ物
      var tweetText : String = status.getText() //ツイート本文の取得
//      var tweetUser : String = status.getUser.getName
//      var tweetLat : String = "Null"
//      var tweetLong : String = "Null"
//      var tweetPref : String = SplitAddress.splitaddress(tweetText)
//      if(status.getGeoLocation != null) {
//        tweetLat= status.getGeoLocation.getLatitude.toString
//        tweetLong = status.getGeoLocation.getLongitude.toString
//      }
//      var tweetTextArrayGeo: Array[String] = new Array[String](5)
//      tweetTextArrayGeo(0) = "@"+tweetUser
//      tweetTextArrayGeo(1) = tweetText
//      tweetTextArrayGeo(2) = tweetLat
//      tweetTextArrayGeo(3) = tweetLong
//      tweetTextArrayGeo(4) = tweetPref
//
//      var tweetTextArray : Array[String] = new Array[String](2)
//      tweetTextArray(0) = "@"+tweetUser
//      tweetTextArray(1) = tweetText
//      Csvperser.writeToCsvFile(tweetTextArray)
//      if(status.getGeoLocation != null){
//        Csvperser.writeToCsvFileGeo(tweetTextArrayGeo)
//      }
//      Csvperser.writeToCsvFile(tweetTextArray)
      print(tweetText)
      val japanese_pattern : Pattern = Pattern.compile("[¥¥u3040-¥¥u309F]+") //「ひらがなが含まれているか？」の正規表現

      if(status.getGeoLocation != null
        && (status.getGeoLocation.getLatitude >= 28.4 && status.getGeoLocation.getLatitude <= 46.20)
        && (status.getGeoLocation.getLongitude >= 129.5 && status.getGeoLocation.getLongitude <= 146.1)){
        //ファイル書き出し
        var tweetUser : String = status.getUser.getName
        var tweetLat : String = "Null"
        var tweetLong : String = "Null"
        var tweetPref : String = SplitAddress.splitaddress(tweetText)
        var tweetTime : String = status.getCreatedAt.toString
        if(status.getGeoLocation != null) {
          tweetLat= status.getGeoLocation.getLatitude.toString
          tweetLong = status.getGeoLocation.getLongitude.toString
        }
        var tweetTextArrayGeo: Array[String] = new Array[String](6)
        tweetTextArrayGeo(0) = "@"+tweetUser
        tweetTextArrayGeo(1) = tweetText
        tweetTextArrayGeo(2) = tweetPref
        tweetTextArrayGeo(3) = tweetTime
        tweetTextArrayGeo(4) = tweetLat
        tweetTextArrayGeo(5) = tweetLong


        if(status.getGeoLocation != null){
          Csvperser.writeToCsvFileGeo(tweetTextArrayGeo)
        }
        //
      }
      if(japanese_pattern.matcher(tweetText).find()) {  // ひらがなが含まれているツイートのみ処理
        // 不要な文字列の削除
        tweetText = tweetText.replaceAll("http(s*)://(.*)/", "").replaceAll("¥¥uff57", "") // 全角の「ｗ」は邪魔www
        // ツイート本文の解析


        //書き出し
        var tweetTextArray : Array[String] = new Array[String](2)
        var tweetUser : String = status.getUser.getName
        var tweetTime : String = status.getCreatedAt.toString
        tweetTextArray(0) = "@"+tweetUser
        tweetTextArray(1) = tweetText
        tweetTextArray(2) = tweetTime
        Csvperser.writeToCsvFile(tweetTextArray)
        //



        val tokens : java.util.List[Token] = CustomTwitterTokenizer4.tokenize(tweetText, dictFilePath)
        val pattern : Pattern = Pattern.compile("^[a-zA-Z]+$|^[0-9]+$") //「英数字か？」の正規表現
        for(index <- 0 to tokens.size()-1) { //各形態素に対して。。。
        val token = tokens.get(index)

          // 英単語文字を排除したい場合はこれを使う
          val matcher : Matcher = pattern.matcher(token.getSurfaceForm())

          if(token.getSurfaceForm().length() >= 2 && !matcher.find()) {
            //わいは今はcsvの列を数えているけど，これで「I'm at」を含んで，正規表現にマッチした都道府県のカウントを行えばよい？
            if (tokens.get(index).getAllFeaturesArray()(0) == "名詞" && (tokens.get(index).getAllFeaturesArray()(1) == "一般" || tokens.get(index).getAllFeaturesArray()(1) == "固有名詞")) {
              features += tokens.get(index).getSurfaceForm

              println(tokens.get(index).getAllFeaturesArray()(1))
            } else if (tokens.get(index).getPartOfSpeech == "カスタム名詞") {
              println(tokens.get(index).getPartOfSpeech)
              // println(tokens.get(index).getSurfaceForm)
              features += tokens.get(index).getSurfaceForm
            }
          }
        }
      }
      (features)
    })

    // ソート方法を定義（必ずソートする前に定義）
    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = a.compare(b)*(-1)
    }


    // ウインドウ集計（行末の括弧の位置はコメントを入れるためです、気にしないで下さい。）
    val topCounts60 = tweetStream.map((_, 1)                      // 出現回数をカウントするために各単語に「1」を付与
    ).reduceByKeyAndWindow(_+_, Seconds(5*60)   // ウインドウ幅(60*60sec)に含まれる単語を集める
    ).map{case (topic, count) => (count, topic)  // 単語の出現回数を集計
    }.transform(_.sortByKey(true))               // ソート


    // TODO スコアリングはタイトルで1つに集計しなおす必要がある
    // TODO 俺ガイル, oregaisu => 正式タイトルに直して集計


    // 出力
    topCounts60.foreachRDD(rdd => {
      // 出現回数上位20単語を取得

      // ソート
      // val rddSort = rdd.map(x => (x,1)).reduceByKey((x,y) => x + y).sortBy(_._2)

      val sendMsg = new StringBuilder()

      val topList = rdd.take(takeRankNum)
      // コマンドラインに出力
      println("¥ nPopular topics in last 60*60 seconds (%s words):".format(rdd.count()))
      topList.foreach { case (count, tag) =>
        println("%s (%s tweets)".format(tag, count))
        sendMsg.append("%s:%s,".format(tag, count))
      }
      // Send Msg to Kafka
      // TOPスコア順にワードを送信
      println(sendMsg.toString())
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
object CustomTwitterTokenizer4 {
  def tokenize(text: String, dictPath: String): java.util.List[Token] = {
    Tokenizer.builder().mode(Tokenizer.Mode.SEARCH)
      .userDictionary(dictPath)
      .build().tokenize(text)
  }
}

object SplitAddress{
  def splitaddress(string: String): String ={
    var pref = "(\\s...??[都県]|北海道|京都府|大阪府)"
    var returnPref = ""
    var p : Pattern = Pattern.compile(pref)
    var m : Matcher = p.matcher(string)
    if(m.find()){
      returnPref = m.group(1)
      returnPref = returnPref.trim
    }
    (returnPref)
  }
}

object Csvperser{
  def opencsvToStringArray(args:Array[String]): Unit ={

  }
  def opencsvToBean(): Unit ={

  }
  def readFromCsvFile(): Unit ={

  }
  def writeToCsvFileGeo(args: Array[String]): Unit ={

    var writer = new CSVWriter(new FileWriter("output/outPutGeo.csv",true))
    var strArr = args
    writer.writeNext(strArr)
    writer.flush()
  }
  def writeToCsvFile(args: Array[String]): Unit ={

    var writer = new CSVWriter(new FileWriter("output/outPut.csv",true))
    var strArr = args
    writer.writeNext(strArr)
    writer.flush()
  }
//  def isFileValid(): Unit ={
//
//  }
}

//これは今回は使うのが難しいとわかったので，今後ローカルジオコーディングを実装する際の雛形にする．
//object YahooReverseGeoCoder{
//  def setter: Unit ={
//
//  }
//}