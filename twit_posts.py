import tweepy
import json
import time
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Replace with your Bearer Token
bearer_token = "AAAAAAAAAAAAAAAAAAAAAKPG2gEAAAAAHBJ4Y0OWSDIJY8E0qKByfFzAXt0%3DPCzdFrLI0GgH0pUY7Y821Kjyus1yLHGii7CmpFH0trflfZvKu1"

# Initialize client with wait_on_rate_limit
client = tweepy.Client(
    bearer_token=bearer_token,
    wait_on_rate_limit=True  # Automatically handle rate limits
)

# Configuration
keyword = "Čikaga"
max_results_per_request = 10  # Reduced to be more conservative
total_limit = 100  # Reduced for testing
output_file = "tweets_data.json"

def scrape_tweets_with_backoff():
    """Scrape tweets with exponential backoff strategy"""
    tweets_data = []
    next_token = None
    retry_count = 0
    max_retries = 5
    base_delay = 60  # Start with 1 minute delay
    
    try:
        while len(tweets_data) < total_limit:
            try:
                logger.info(f"Fetching tweets... Current count: {len(tweets_data)}")
                
                tweets = client.search_recent_tweets(
                    query=keyword,
                    max_results=max_results_per_request,
                    tweet_fields=["created_at", "text", "public_metrics", "entities"],
                    user_fields=["username", "name", "verified"],
                    expansions=["author_id", "attachments.media_keys"],
                    media_fields=["type", "url"],
                    start_time=datetime.now() - timedelta(days=7),  # Last 7 days
                    next_token=next_token
                )
                
                if tweets.data:
                    for tweet in tweets.data:
                        # Get user info safely
                        user_info = {"username": None, "name": None, "verified": None}
                        if tweets.includes and "users" in tweets.includes:
                            for user in tweets.includes["users"]:
                                if user.id == tweet.author_id:
                                    user_info = {
                                        "username": user.username,
                                        "name": user.name,
                                        "verified": getattr(user, 'verified', None)
                                    }
                                    break
                        
                        # Get media info safely
                        media_info = []
                        if (tweets.includes and "media" in tweets.includes and 
                            tweet.attachments and "media_keys" in tweet.attachments):
                            for media in tweets.includes["media"]:
                                if media.media_key in tweet.attachments["media_keys"]:
                                    media_info.append({
                                        "type": media.type,
                                        "url": getattr(media, 'url', None)
                                    })
                        
                        tweet_data = {
                            "id": tweet.id,
                            "created_at": str(tweet.created_at),
                            "text": tweet.text,
                            "public_metrics": tweet.public_metrics if tweet.public_metrics else {},
                            "author": user_info,
                            "media": media_info
                        }
                        tweets_data.append(tweet_data)
                        
                        if len(tweets_data) >= total_limit:
                            break
                    
                    # Handle pagination
                    next_token = tweets.meta.get("next_token") if len(tweets.data) == max_results_per_request else None
                    if not next_token:
                        logger.info("No more tweets available")
                        break
                    
                    # Reset retry count on success
                    retry_count = 0
                    
                    # Add small delay between requests
                    time.sleep(2)
                    
                else:
                    logger.info(f"No more tweets found for keyword '{keyword}'")
                    break
                    
            except tweepy.TweepyException as e:
                if "429" in str(e):
                    retry_count += 1
                    if retry_count > max_retries:
                        logger.error("Max retries exceeded. Stopping.")
                        break
                    
                    delay = base_delay * (2 ** retry_count)  # Exponential backoff
                    logger.warning(f"Rate limit hit. Waiting {delay} seconds... (Retry {retry_count}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"API Error: {e}")
                    break
            
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                break
        
        # Save data to file
        if tweets_data:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(tweets_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(tweets_data)} tweets to {output_file}")
        
        return tweets_data
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return tweets_data

def display_tweet_summary(tweets_data):
    """Display a summary of collected tweets"""
    if not tweets_data:
        print("No tweets collected.")
        return
    
    print(f"\n=== TWEET SUMMARY ===")
    print(f"Total tweets collected: {len(tweets_data)}")
    print(f"Keyword: {keyword}")
    print(f"Data saved to: {output_file}")
    
    print(f"\n=== SAMPLE TWEETS ===")
    for i, tweet in enumerate(tweets_data[:3]):  # Show first 3 tweets
        print(f"\nTweet {i+1}:")
        print(f"Author: @{tweet['author']['username']} ({tweet['author']['name']})")
        print(f"Created: {tweet['created_at']}")
        print(f"Text: {tweet['text'][:100]}{'...' if len(tweet['text']) > 100 else ''}")
        print(f"Likes: {tweet['public_metrics'].get('like_count', 0)}")
        print(f"Retweets: {tweet['public_metrics'].get('retweet_count', 0)}")

if __name__ == "__main__":
    print(f"Starting Twitter scraper for keyword: '{keyword}'")
    print(f"Target: {total_limit} tweets")
    
    tweets_data = scrape_tweets_with_backoff()
    display_tweet_summary(tweets_data)