# 7b46a18bcd28b4f1b847b9c2cac53b0d
from facebook_scraper import get_posts
import json
import time
import logging
from datetime import datetime

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
output_file = "fb_posts_data.json"
total_limit = 10  # Set limit on total posts to collect
max_retries = 3

def scrape_fb_posts(keyword):
    """Scrape public Facebook posts matching a keyword using facebook-scraper"""
    posts_data = []
    retry_count = 0
    
    try:
        logger.info(f"Starting Facebook scraping for keyword: {keyword}")

        for post in get_posts(
            pages=[keyword],  # Here keyword must be a Facebook Page/Group name
            cookies="cookies.txt",  # Optional: add logged-in session cookie
            extra_info=True
        ):
            try:
                logger.info(f"Scraped post ID: {post['post_id']}")

                post_data = {
                    "_id": str(post["post_id"]),
                    "text": post.get("text", "N/A"),
                    "post_url": post.get("post_url", "N/A"),
                    "username": post.get("username", "N/A"),
                    "handle": post.get("user_id", "N/A"),
                    "images": post.get("images", []),
                    "video": post.get("video", "N/A"),
                    "likes": post.get("likes", 0),
                    "comments": post.get("comments", 0),
                    "shares": post.get("shares", 0),
                    "timestamp": post.get("time").isoformat() if post.get("time") else "N/A",
                    "time_text": post.get("time").strftime("%I:%M %p · %b %d, %Y") if post.get("time") else "N/A",
                    "source_type": "social network",
                    "source_domain": "facebook.com",
                    "content_type": "social post",
                    "keyword": keyword
                }

                posts_data.append(post_data)

                if len(posts_data) >= total_limit:
                    break

            except Exception as e:
                logger.error(f"Error processing post: {e}")
                continue
        
        # Save to file
        if posts_data:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(posts_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(posts_data)} posts to {output_file}")
        
        return posts_data

    except Exception as e:
        logger.error(f"Fatal scraping error: {e}")
        return posts_data

def display_fb_summary(posts_data):
    if not posts_data:
        print("No Facebook posts collected.")
        return

    print(f"\n=== FACEBOOK POST SUMMARY ===")
    print(f"Total posts collected: {len(posts_data)}")
    print(f"Data saved to: {output_file}")

    print(f"\n=== SAMPLE POSTS ===")
    for i, post in enumerate(posts_data[:3]):
        print(f"\nPost {i+1}:")
        print(f"Author: {post['handle']}")
        print(f"Created: {post['time_text']}")
        print(f"Text: {post['text'][:100]}{'...' if len(post['text']) > 100 else ''}")
        print(f"Likes: {post['likes']}, Comments: {post['comments']}, Shares: {post['shares']}")

if __name__ == "__main__":
    keyword = input("Enter the Facebook Page/Group username to scrape: ").strip()
    if not keyword:
        print("No keyword provided. Exiting.")
        exit()

    posts_data = scrape_fb_posts(keyword)
    display_fb_summary(posts_data)
