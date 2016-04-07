import java.io._
import java.util
import java.util.Properties
import java.util.regex.{Matcher, Pattern}

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Text
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.atilika.kuromoji.{Token, Tokenizer}

import scala.collection.mutable.ListBuffer
import scala.io.Source

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

    //    北海道,青森県,岩手県,宮城県,秋田県,山形県,福島県,茨城県,栃木県,群馬県,埼玉県,\
    //    千葉県,東京都,神奈川県,新潟県,富山県,石川県,福井県,山梨県,長野県,岐阜県,静岡県,愛知県,三重県,滋賀県,京都府,\
    //    大阪府,兵庫県,奈良県,和歌山県,鳥取県,島根県,岡山県,広島県,山口県,徳島県,香川県,愛媛県,高知県,福岡県,佐賀県,\
    //    長崎県,熊本県,大分県,宮崎県, 鹿児島県,沖縄県

    //ワードサーチあり
    val stream = TwitterUtils.createStream(ssc, None, searchWordList)
    //ワードサーチなし
    //val stream = TwitterUtils.createStream(ssc, None)
    // Twitterから取得したツイートを処理する
    //val tweetGeoStream = stream.filter()
    val tweetStream = stream.flatMap(status => {

      //      val tokenizer : Tokenizer = CustomTwitterTokenizer.builder().build()  // kuromojiの分析器
      val features: scala.collection.mutable.ArrayBuffer[String] = new collection.mutable.ArrayBuffer[String]() //解析結果を保持するための入れ物
      var tweetText: String = status.getText() //ツイート本文の取得
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
      var tweetPref: String = SplitAddress.splitaddress(tweetText)
      println("抜き出した都道府県 :" + tweetPref + "\n")
      print(tweetText + "\n")
      tweetText = SplitAddress.splitcheckin(tweetText)
      print(tweetText + "\n")
      val japanese_pattern: Pattern = Pattern.compile("[¥¥u3040-¥¥u309F]+") //「ひらがなが含まれているか？」の正規表現

      if (status.getGeoLocation != null
        && (status.getGeoLocation.getLatitude >= 28.4 && status.getGeoLocation.getLatitude <= 46.20)
        && (status.getGeoLocation.getLongitude >= 129.5 && status.getGeoLocation.getLongitude <= 146.1)) {
        //ファイル書き出し
        var tweetUser: String = status.getUser.getName
        var tweetLat: String = "Null"
        var tweetLong: String = "Null"
        var tweetPref: String = SplitAddress.splitaddress(tweetText)
        var tweetTime: String = status.getCreatedAt.toString
        if (status.getGeoLocation != null) {
          tweetLat = status.getGeoLocation.getLatitude.toString
          tweetLong = status.getGeoLocation.getLongitude.toString
        }
        var tweetTextArrayGeo: Array[String] = new Array[String](6)
        tweetTextArrayGeo(0) = "@" + tweetUser
        tweetTextArrayGeo(1) = tweetText
        tweetTextArrayGeo(2) = tweetPref
        tweetTextArrayGeo(3) = tweetTime
        tweetTextArrayGeo(4) = tweetLat
        tweetTextArrayGeo(5) = tweetLong
        //if(status.getGeoLocation != null){
        Csvperser.writeToCsvFileGeo(tweetTextArrayGeo)
        //}
        //ファイル書き出し
      }
      if (japanese_pattern.matcher(tweetText).find()) {
        // ひらがなが含まれているツイートのみ処理
        // 不要な文字列の削除
        tweetText = tweetText.replaceAll("http(s*)://(.*)/", "").replaceAll("¥¥uff57", "") // 全角の「ｗ」は邪魔www
        // ツイート本文の解析

        //ファイル書き出しnonGeo
        //        var tweetTextArray : Array[String] = new Array[String](3)
        //        var tweetUser : String = status.getUser.getName
        //        var tweetTime : String = status.getCreatedAt.toString
        //        tweetTextArray(0) = "@"+tweetUser
        //        tweetTextArray(1) = tweetText
        //        tweetTextArray(2) = tweetTime
        //        Csvperser.writeToCsvFile(tweetTextArray)
        //ファイル書き出しnonGeo


        var tweetCity: String = SplitAddress.splitaddresscity(tweetText)
        val tokens: java.util.List[Token] = CustomTwitterTokenizer4.tokenize(tweetText, dictFilePath)
        val pattern: Pattern = Pattern.compile("^[a-zA-Z]+$|^[0-9]+$") //「英数字か？」の正規表現
        for (index <- 0 to tokens.size() - 1) {
          //各形態素に対して。。。
          val token = tokens.get(index)

          // 英単語文字を排除したい場合はこれを使う
          val matcher: Matcher = pattern.matcher(token.getSurfaceForm())

          if (token.getSurfaceForm().length() >= 2 && !matcher.find()) {
            //わいは今はcsvの列を数えているけど，これで「I'm at」を含んで，正規表現にマッチした都道府県のカウントを行えばよい？
            if (tokens.get(index).getAllFeaturesArray()(0) == "名詞" && (tokens.get(index).getAllFeaturesArray()(1) == "一般" || tokens.get(index).getAllFeaturesArray()(1) == "固有名詞")) {
              //ここで都道府県と連結
              if (features != tweetPref && features != "日本") {
                features += tweetPref + ":" + tokens.get(index).getSurfaceForm
              }
              println(tokens.get(index).getAllFeaturesArray()(1))
            } else if (tokens.get(index).getPartOfSpeech == "カスタム名詞") {
              println(tokens.get(index).getPartOfSpeech)
              println(tokens.get(index).getSurfaceForm)
              //ここで都道府県と連結
              if (features != tweetPref && features != "日本") {
                //if(features != tweetPref && features != "日本" && features != tweetCity) {
                features += tweetPref + ":" + tweetPref + tokens.get(index).getSurfaceForm
              }
            }
          }
        }
      }
      (features)
    })

    // ソート方法を定義（必ずソートする前に定義）
    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = a.compare(b) * (-1)
    }

    // ウインドウ集計（行末の括弧の位置はコメントを入れるためです、気にしないで下さい。）
    val topCounts60 = tweetStream.map((_, 1) // 出現回数をカウントするために各単語に「1」を付与
    ).reduceByKeyAndWindow(_ + _, Seconds(5 * 60) // ウインドウ幅(60*60sec)に含まれる単語を集める
    ).map { case (topic, count) => (count, topic) // 単語の出現回数を集計
    }.transform(_.sortByKey(true)) // ソート

    // TODO スコアリングはタイトルで1つに集計しなおす必要がある
    // TODO 俺ガイル, oregaisu => 正式タイトルに直して集計

    // 出力
    topCounts60.foreachRDD(rdd => {
      // 出現回数上位20単語を取得

      // ソート
      // val rddSort = rdd.map(x => (x,1)).reduceByKey((x,y) => x + y).sortBy(_._2)

      val sendMsg = new StringBuilder()

      val topList = rdd.take(takeRankNum)
      var topListCsv: Array[String] = new Array[String](topList.length)
      var minusCnt = 0
      // コマンドラインに出力
      println("¥ nPopular topics in last 60*60 seconds (%s words):".format(rdd.count()))
      topList.foreach { case (count, tag) =>
        println("%s (%s tweets)".format(tag, count))
        sendMsg.append("%s,%s".format(tag, count))
        var tmp = "\"" + tag + "\"" + "," + "\"" + count + "\""
        //println(tmp)
        topListCsv(minusCnt) = tmp
        minusCnt += 1
      }
      // Send Msg to Kafka
      // TOPスコア順にワードを送信
      println(sendMsg.toString())
      //var tmp: Array[String] = new Array[String](topList.length)
      //      var tmp: Array[String] = sendMsg.toString().split("\t")
      //      Csvperser.writeToCsvFile(tmp)
      Csvperser.writeToCsvFile(topListCsv)

      CreateOutputCsv.createTweetOfPrefecture

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

object SplitAddress {
  def splitaddress(string: String): String = {
    //var pref = "(\\s..??[県]|\\s...??[県]|北海道|東京都|京都府|大阪府)"
    var pref = "(\\s..??[県]|北海道|東京都|京都府|大阪府|神奈川県|和歌山県|鹿児島県)"
    //4文字の県だけで正規表現でひっかけていると頭に何かついたのも拾ってきてしまう
    //採取県www
    var returnPref = ""
    val p: Pattern = Pattern.compile(pref)
    var m: Matcher = p.matcher(string)
    if (m.find()) {
      returnPref = m.group(1)
      returnPref = returnPref.trim
    }
    returnPref
  }

  def splitaddresscity(string: String): String = {
    var returncity = ""

    //var city = "(東京|茨城|埼玉|栃木|福岡|神奈川|大阪|愛知|兵庫|北海道|札幌|青森|盛岡|仙台|秋田|山形|福島|水戸|宇都宮|前橋|さいたま|千葉|新宿|横浜|新潟|富山|金沢|福井|甲府|長野|岐阜|静岡|名古屋|津|大津|京都|大阪|神戸|奈良|和歌山|鳥取|松江|岡山|広島|山口|徳島|高松|松山|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|那覇)"
    val city = Array("北海道", "青森", "岩手", "宮城", "秋田", "山形", "福島", "茨城", "栃木", "群馬", "埼玉", "千葉",
      "東京", "神奈川", "新潟", "富山", "石川", "福井", "山梨", "長野", "岐阜", "静岡", "愛知", "三重", "滋賀", "京都",
      "大阪", "兵庫", "奈良", "和歌山", "鳥取", "島根", "岡山", "広島", "山口", "徳島", "香川", "愛媛", "高知", "福岡",
      "佐賀", "長崎", "熊本", "大分", "宮崎", "鹿児島", "沖縄")

    (returncity)
  }

  def splitcheckin(string: String): String = {
    var returncheckin = ""
    var exp = "I'm at (.+?) in"
    var matchstr = ""
    //    val javalist: java.util.List[String] = new util.ArrayList[String](10)//ていうかリストじゃなくてよくねｗ
    //    val scalalist: ListBuffer[String] = ListBuffer()
    val p: Pattern = Pattern.compile(exp)
    var m: Matcher = p.matcher(string)
    //while (m.find()) {
    if (m.find()) {
      matchstr = m.group(1)
    }
    if (matchstr != null) {
      returncheckin = matchstr
      returncheckin = returncheckin.trim
    } else {
      returncheckin = ""
    }
    returncheckin
  }

  def getaddress(tweet: String, pref: String): String = {
    //var pref = "(\\s..??[県]|\\s...??[県]|北海道|東京都|京都府|大阪府)"
    var pref = "(\\s..??[県]|北海道|東京都|京都府|大阪府|神奈川県|和歌山県|鹿児島県)"
    //4文字の県だけで正規表現でひっかけていると頭に何かついたのも拾ってきてしまう
    //採取県www
    var tweetOfPrefectures = ""
    //var swarm = "I'm at"
    val p: Pattern = Pattern.compile(pref)
    var m: Matcher = p.matcher(tweet)
    if (m.find()) {
      tweetOfPrefectures = m.group(0)
      tweetOfPrefectures = tweetOfPrefectures.trim
    }
    tweetOfPrefectures
  }

  def checkprefectures(tweet: String, pref: String): Boolean = {
    //var prefecture = "(\\s..??[県]|\\s...??[県]|北海道|東京都|京都府|大阪府)"
    var prefecture = "(\\s..??[県]|北海道|東京都|京都府|大阪府|神奈川県|和歌山県|鹿児島県)"
    var boolean = false
    var tweetOfPrefectures = ""
    val p: Pattern = Pattern.compile(prefecture)
    var m: Matcher = p.matcher(tweet)
    if (m.find()) {
      tweetOfPrefectures = m.group(0)
      tweetOfPrefectures = tweetOfPrefectures.trim
    }
    if(pref == tweetOfPrefectures){
      boolean = true
    }else{
      boolean = false
    }
    boolean
  }

}

//object SplitAddressAndSwarm{
//  def splitaddress(string: String): String ={
//    var pref = "(\\s...??[都県]|北海道|京都府|大阪府)"
//    var returnPref = ""
//    var swarm = "I'm at"
//    var p : Pattern = Pattern.compile(pref)
//    var m : Matcher = p.matcher(string)
//    var s : Matcher = p.matcher(swarm)
//    if(m.find() && s.find()){
//      returnPref = m.group(1)
//      returnPref = returnPref.trim
//    }
//    (returnPref)
//  }
//}


object Csvperser {
  def opencsvToStringArray(args: Array[String]): Unit = {

  }

  def opencsvToBean(): Unit = {

  }

  def readFromCsvFile(): Unit = {

  }

  def writeToCsvFileGeo(args: Array[String]): Unit = {

    var writer = new CSVWriter(new FileWriter("output/outPutGeo.csv", true))
    var strArr = args
    writer.writeNext(strArr)
    writer.flush()
  }

  def writeToCsvFile(args: Array[String]): Unit = {

    //めんどくさいし,FileReaderで1行ずつ書き込もう
    //    var writer = new CSVWriter(new FileWriter("output/outPut.csv", false))
    //    var strArr = args
    //    writer.writeNext(strArr)
    //    writer.flush()

    var fos = new FileOutputStream("op.csv")
    var osw = new OutputStreamWriter(fos, "SJIS")

    var fileWriter = new FileWriter("output/tweetLogFile.csv", false)
    var bufferedWriter = new BufferedWriter(fileWriter)
    for (tweet <- args) {
      bufferedWriter.write(tweet.toString())
      bufferedWriter.newLine()
    }
    bufferedWriter.close()
  }

  //  def isFileValid(): Unit ={
  //
  //  }
}


object CreateOutputCsv {
  def createTweetOfPrefecture: Unit = {
    val prefectures = Array("北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県", "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県",
      "東京都", "神奈川県", "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県", "三重県", "滋賀県", "京都府",
      "大阪府", "兵庫県", "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県", "徳島県", "香川県", "愛媛県", "高知県", "福岡県",
      "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県")

    var source = Source.fromFile("output/tweetLogFile.csv")
    //var fileLines = source.getLines.toList
    var fileLines = source.getLines.toArray
    //var tweetOfPrefectures: Array[String] = new Array[String](fileLines.length)

    val TweetArray = fileLines.filterNot(_.isEmpty).map { line =>
      (line.toArray).filter(e => e != ' ')
      //(line.toList).filter(e => e != ' ')
    }.toArray
    //    println("arrayのチェック\n")
    //    println(TweetArray.deep.mkString("\n"))
    //各都道府県のループ
    //格納されたツイートのループ
    for (cntP <- 0 to prefectures.length-1; cntT <- 0 to TweetArray.length-1) yield {
//      println("\n")
//      println(prefectures(cntP) +" : "+ TweetArray(cntT).toString())
      var tmpBool = false
      tmpBool = SplitAddress.checkprefectures(TweetArray(cntT).mkString(""),prefectures(cntP))
      if(tmpBool == true){
        println("true\n")
        println(prefectures(cntP)+"\n")
        println(TweetArray(cntT).mkString(""))
      }else{
        //println("false\n")
        //println(TweetArray(cntT).mkString(""))
      }
    }
    source.close
  }
}

//これは今回は使うのが難しいとわかったので，今後ローカルジオコーディングを実装する際の雛形にする．
//object YahooReverseGeoCoder{
//  def setter: Unit ={
//
//  }
//}
