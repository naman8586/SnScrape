import tweepy
import json

# Replace with your Bearer Token
bearer_token = "AAAAAAAAAAAAAAAAAAAAAKPG2gEAAAAAHBJ4Y0OWSDIJY8E0qKByfFzAXt0%3DPCzdFrLI0GgH0pUY7Y821Kjyus1yLHGii7CmpFH0trflfZvKu1"

# Initialize client
client = tweepy.Client(bearer_token=bearer_token)

# Tweet ID to scrape
tweet_id = "1936873808167600625"

# Fetch the specific tweet
try:
    tweet = client.get_tweet(
        id=tweet_id,
        expansions=["author_id", "attachments.media_keys"],
        tweet_fields=["created_at", "text", "public_metrics", "entities"],
        user_fields=["username", "name", "verified"],
        media_fields=["type", "url"]
    )

    if tweet.data:
        tweet_data = {
            "id": tweet.data.id,
            "created_at": str(tweet.data.created_at),
            "text": tweet.data.text,
            "public_metrics": tweet.data.public_metrics,
            "author": {
                "username": tweet.includes["users"][0].username if tweet.includes and tweet.includes["users"] else None,
                "name": tweet.includes["users"][0].name if tweet.includes and tweet.includes["users"] else None,
                "verified": tweet.includes["users"][0].verified if tweet.includes and tweet.includes["users"] else None
            },
            "media": [
                {
                    "type": media.type,
                    "url": media.url
                } for media in tweet.includes["media"] if tweet.includes and tweet.includes["media"]
            ]
        }
        print(json.dumps(tweet_data, indent=2))
    else:
        print(f"Tweet with ID {tweet_id} not found or inaccessible.")
except tweepy.TweepyException as e:
    print(f"Error: {e}")