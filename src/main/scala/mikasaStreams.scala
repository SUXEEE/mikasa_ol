//public static void main(String[] args) throws TwitterException {
//  TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
//
//  String prefectures[] = { "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県", "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県",
//    "東京都", "神奈川県", "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県", "三重県", "滋賀県", "京都府",
//    "大阪府", "兵庫県", "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県", "徳島県", "香川県", "愛媛県", "高知県", "福岡県",
//    "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県" };
//
//  String userProfile = System.getProperty("user.home");
//  FileOutputStream fos = null;
//  BufferedWriter bw = null;
//  File file = new File(userProfile + "/Documents/GitHub/JPTweetD3/tweetData/tweetOfPrefecture.csv");
//
//  		 try {
//  		 // 出力先を作成する
//  		 fos = new FileOutputStream(userProfile +
//  		 "/Documents/GitHub/JPTweetD3/tweetData/tweetOfPrefecture.csv");
//  		 fos.write(0xef);
//  		 fos.write(0xbb);
//  		 fos.write(0xbf);
//  		 bw = new BufferedWriter(new OutputStreamWriter(fos, "UTF8"));
//  		 // bw.write("日本語, Alphabet, カタカナ, 1\r\n");
//  		 bw.write("ken,tweetAll,extra,tweet1,tweet2,tweet3\r\n");
//  		 for (String pre : prefectures) {
//  		 bw.write(pre + ",");
//  		 bw.write(0 + "\r\n");
//  		 }
//  		 } catch (IOException ex) {
//  		 // 例外時処理
//  		 ex.printStackTrace();
//  		 } finally {
//  		 if (bw != null) {
//  		 try {
//  		 bw.close();
//  		 } catch (Exception e) {
//  		 e.printStackTrace();
//  		 }
//  		 }
//  		 if (fos != null) {
//  		 try {
//  		 fos.close();
//  		 } catch (Exception e) {
//  		 e.printStackTrace();
//  		 }
//  		 }
//  		 }
//
//  StatusListener listener = new StatusListener() {
//    @Override
//    public void onStatus(Status status) {
//      Double lat = null;
//      Double lng = null;
//      String[] urls = null;
//      String[] medias = null;
//
//      GeoLocation location = status.getGeoLocation();
//
//      if (location != null) {
//        double dlat = location.getLatitude();
//        double dlng = location.getLongitude();
//        lat = dlat;
//        lng = dlng;
//      }
//      long id = status.getId();
//      String text = status.getText();
//      String prefecture = null;
//
//
//      List<String[]> str = Csvparser.opencsvToStringArray(file);
//      //System.out.println("suxee: " + str.get(1)[0]);
//
//      if (lat != null && lng != null) {
//        System.out.println("Split Address");
//        prefecture = SplitAddress.splitAddress(text);
//        // System.out.println(prefecture);
//        //					for(int i = 1; i < 48 ; i++){
//        //						if(prefecture.equals(str.get(i)[0])){
//        //							System.out.println(str.get(i)[0]+"がcsvから抜き出したList<String>と一致しました，加算します");
//        //						}
//        //					}
//      }
//
//
//
//      StringBuilder strbText = new StringBuilder();
//      long userid = status.getUser().getId();
//      String username = status.getUser().getScreenName();
//      Date created = status.getCreatedAt();
//
//      if (location != null) {
//        System.out.println("geolocation" + "\n" + "lat = " + lat + "\n" + "long = " + lng + "\n"
//          + "username = " + username + "\n" + "text = " + text + "\n" + "都道府県 = " + prefecture + "\n"
//        );
//        for(int i = 1; i < 48 ; i++){
//          if(prefecture != null && prefecture.equals(str.get(i)[0])){
//            System.out.println(str.get(i)[0]+"がcsvから抜き出したList<String>と一致しました，加算します");
//          }
//        }
//
//
//      } else {
//        System.out.println("No geo loc.");
//      }
//
//      if (lat != null && lng != null && prefecture == null) {
//        YahooReverseGeoCoder.setter(lat, lng);
//      } else {
//        System.out.println("No geo loc.");
//      }
//    }
//
//    @Override
//    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
//      System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
//    }
//
//    @Override
//    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
//      System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
//    }
//
//    @Override
//    public void onScrubGeo(long userId, long upToStatusId) {
//      System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
//    }
//
//    @Override
//    public void onStallWarning(StallWarning warning) {
//      System.out.println("Got stall warning:" + warning);
//    }
//
//    @Override
//    public void onException(Exception ex) {
//      ex.printStackTrace();
//    }
//  };
//  twitterStream.addListener(listener);
//  // twitterStream.sample();
//  // String[] track = { "東京" };
//  // double[][] locations = {new double[]{132.2,29.9},new
//  // double[]{146.1,46.20}};
//  double[][] locations = { new double[] { 129.5, 28.4 }, new double[] { 146.1, 46.20 } };
//  FilterQuery filter = new FilterQuery();
//  // filter.track( track );
//  filter.locations(locations);
//  twitterStream.filter(filter);
//
//}