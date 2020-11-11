package Api_Test
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder

object Runner {

  def main(args : Array[String]) {

    // (1) config work to create a twitter object
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey("") // Just copy and paste inside the quotes and same for below
      .setOAuthConsumerSecret("")
      .setOAuthAccessToken("")
      .setOAuthAccessTokenSecret("")
    val tf = new TwitterFactory(cb.build)
    val twitter = tf.getInstance

    // (2) use the twitter object to get your friend's timeline
    val statuses = twitter.showUser("TotalTrafficNYC")
    println(statuses.getId)


  }

}
