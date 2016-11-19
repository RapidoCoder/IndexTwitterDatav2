package com.xululabs.twitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class Twitter4jApi {

	Query query;
	QueryResult result;

	/**
	 * use to get twitter instance
	 * 
	 * @return Twiiter Instance
	 */
	public Twitter getTwitterInstance(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) throws Exception {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey(consumerKey)
				.setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken)
				.setOAuthAccessTokenSecret(accessTokenSecret);
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		return twitter;
	}

	public ArrayList<Map<String, Object>> search(Twitter twitter, String keyword)
			throws Exception {
		int searchResultCount = 0;
		long lowestTweetId = Long.MAX_VALUE;
		int tweetsCount = 0;
		int requestsCount = 0;
		ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
		Query query = new Query(keyword);
		query.setCount(100);
		do {
			QueryResult queryResult;
			try {
				queryResult = twitter.search(query);
				searchResultCount = queryResult.getTweets().size();
				requestsCount++;
				for (Status tweet : queryResult.getTweets()) {
					System.out.println( tweet.getText());
					Map<String, Object> tweetInfo = new HashMap<String, Object>();
					tweetInfo.put("id", tweet.getId());
					tweetInfo.put("tweet", tweet.getText());
					tweetInfo
							.put("screenName", tweet.getUser().getScreenName());
					tweetInfo.put("retweetCount", tweet.getRetweetCount());
					tweetInfo.put("followersCount", tweet.getUser()
							.getFollowersCount());
					tweetInfo.put("user_image", tweet.getUser().getProfileImageURL());
					tweetInfo.put("description", tweet.getUser().getDescription());
					tweets.add(tweetInfo);
					tweetsCount++;
					if (tweet.getId() < lowestTweetId) {
						lowestTweetId = tweet.getId();
						query.setMaxId(lowestTweetId);
					}
				}
			} catch (TwitterException e) {
				twitter = null;
				break;
			}
			
		} while (true);
		
		return tweets;

	}

}
