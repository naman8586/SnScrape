import json
import time
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor
import hashlib
import re
import os
import argparse
from aiohttp import ClientSession, ClientTimeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('twitter_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TwitterScraperOptimized:
    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token
        self.max_results_per_request = 100
        self.total_limit = 1000
        self.output_file = "tweets_data.json"
        self.batch_size = 500
        self.tweets_data = []
        self.processed_tweet_ids = set()
        self.rate_limit_reset = 0
        self.requests_remaining = 900
        self.session: Optional[ClientSession] = None

    async def _init_session(self):
        """Initialize aiohttp session"""
        if not self.session:
            timeout = ClientTimeout(total=30)
            self.session = ClientSession(headers={"Authorization": f"Bearer {self.bearer_token}"}, timeout=timeout)

    async def _close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    async def _check_rate_limit_status(self):
        """Check rate limit status using a lightweight endpoint"""
        await self._init_session()
        try:
            async with self.session.get('https://api.twitter.com/2/tweets/sample/stream', params={'tweet.fields': 'id'}) as response:
                if response.status == 200:
                    logger.info("Bearer token appears valid")
                    self.requests_remaining = int(response.headers.get('x-rate-limit-remaining', 900))
                    self.rate_limit_reset = int(response.headers.get('x-rate-limit-reset', 0))
                    logger.info(f"Rate limits: {self.requests_remaining} requests remaining, reset at {datetime.fromtimestamp(self.rate_limit_reset)}")
                    return True
                elif response.status == 429:
                    self.rate_limit_reset = int(response.headers.get('x-rate-limit-reset', time.time() + 900))
                    wait_time = max(self.rate_limit_reset - time.time(), 1)
                    logger.error(f"Rate limit exceeded. Wait until {datetime.fromtimestamp(self.rate_limit_reset)} ({wait_time:.2f} seconds)")
                    return False
                else:
                    error_text = await response.text()
                    logger.error(f"Rate limit check failed: HTTP {response.status}: {error_text}")
                    return False
        except aiohttp.ClientError as e:
            logger.error(f"Rate limit check failed: {e}")
            return False
        finally:
            await self._close_session()

    def _extract_hashtags(self, entities: Optional[Dict]) -> List[str]:
        return [tag['tag'] for tag in entities.get('hashtags', [])]

    def _extract_mentions(self, entities: Optional[Dict]) -> List[str]:
        return [mention['username'] for mention in entities.get('mentions', [])]

    def _extract_urls(self, entities: Optional[Dict]) -> List[str]:
        return [url_data.get('expanded_url', url_data.get('url', '')) for url_data in entities.get('urls', [])]

    def _extract_media_info(self, tweet: Dict, includes: Optional[Dict]) -> Dict[str, Any]:
        media_info = {'images': [], 'videos': [], 'gifs': [], 'polls': []}
        if not (includes and 'media' in includes and tweet.get('attachments', {}).get('media_keys')):
            return media_info
        tweet_media_keys = tweet.get('attachments', {}).get('media_keys', [])
        for media in includes.get('media', []):
            if media.get('media_key') in tweet_media_keys:
                media_url = media.get('url') or media.get('preview_image_url')
                if not media_url:
                    continue
                if media.get('type') == 'photo':
                    media_info['images'].append({
                        'url': media_url,
                        'width': media.get('width'),
                        'height': media.get('height')
                    })
                elif media.get('type') == 'video':
                    media_info['videos'].append({
                        'url': media_url,
                        'duration_ms': media.get('duration_ms'),
                        'width': media.get('width'),
                        'height': media.get('height')
                    })
                elif media.get('type') == 'animated_gif':
                    media_info['gifs'].append({
                        'url': media_url,
                        'width': media.get('width'),
                        'height': media.get('height')
                    })
        return media_info

    def _extract_user_info(self, tweet: Dict, includes: Optional[Dict]) -> Dict[str, Any]:
        user_info = {
            'username': None,
            'name': None,
            'verified': False,
            'followers_count': None,
            'following_count': None,
            'tweet_count': None,
            'profile_image_url': None
        }
        if not (includes and 'users' in includes):
            return user_info
        for user in includes.get('users', []):
            if user.get('id') == tweet.get('author_id'):
                user_info.update({
                    'username': user.get('username'),
                    'name': user.get('name'),
                    'verified': user.get('verified', False),
                    'followers_count': user.get('public_metrics', {}).get('followers_count'),
                    'following_count': user.get('public_metrics', {}).get('following_count'),
                    'tweet_count': user.get('public_metrics', {}).get('tweet_count'),
                    'profile_image_url': user.get('profile_image_url')
                })
                break
        return user_info

    def _extract_keyphrase(self, text: str, max_words: int = 5) -> str:
        if not text:
            return ''
        cleaned_text = re.sub(r'http\S+|www\S+|@\w+', '', text)
        cleaned_text = re.sub(r'[^\w\s#]', '', cleaned_text).strip()
        words = cleaned_text.split()[:max_words]
        return ' '.join(words) if words else ''

    def _calculate_engagement_score(self, metrics: Dict) -> float:
        likes = metrics.get('like_count', 0)
        retweets = metrics.get('retweet_count', 0)
        replies = metrics.get('reply_count', 0)
        quotes = metrics.get('quote_count', 0)
        return (likes * 1) + (retweets * 2) + (replies * 3) + (quotes * 2)

    def _process_tweet(self, tweet: Dict, includes: Optional[Dict], keyword: str) -> Optional[Dict[str, Any]]:
        tweet_id = tweet.get('id')
        if not tweet_id or tweet_id in self.processed_tweet_ids:
            return None
        self.processed_tweet_ids.add(tweet_id)
        user_info = self._extract_user_info(tweet, includes)
        media_info = self._extract_media_info(tweet, includes)
        hashtags = self._extract_hashtags(tweet.get('entities'))
        mentions = self._extract_mentions(tweet.get('entities'))
        urls = self._extract_urls(tweet.get('entities'))
        public_metrics = tweet.get('public_metrics', {})
        metrics = {
            'like_count': public_metrics.get('like_count', 0),
            'retweet_count': public_metrics.get('retweet_count', 0),
            'reply_count': public_metrics.get('reply_count', 0),
            'quote_count': public_metrics.get('quote_count', 0),
            'bookmark_count': public_metrics.get('bookmark_count', 0),
            'impression_count': public_metrics.get('impression_count', 0)
        }
        engagement_score = self._calculate_engagement_score(metrics)
        created_at_str = tweet.get('created_at')
        try:
            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            timestamp_str = created_at.isoformat()
        except (ValueError, TypeError) as e:
            logger.warning(f"Date parsing error for tweet {tweet_id}: {e}")
            created_at = datetime.now()
            timestamp_str = created_at.isoformat()
        unique_id = hashlib.md5(f"{tweet_id}_{keyword}".encode()).hexdigest()
        tweet_data = {
            '_id': unique_id,
            'tweet_id': str(tweet_id),
            'url': f"https://twitter.com/{user_info['username']}/status/{tweet_id}" if user_info['username'] else None,
            'text': tweet.get('text', ''),
            'lang': tweet.get('lang'),
            'possibly_sensitive': tweet.get('possibly_sensitive', False),
            'author_id': str(tweet.get('author_id', '')),
            'username': user_info['username'],
            'name': user_info['name'],
            'handle': f"@{user_info['username']}" if user_info['username'] else None,
            'verified': user_info['verified'],
            'profile_image_url': user_info['profile_image_url'],
            'author_followers_count': user_info['followers_count'],
            'author_following_count': user_info['following_count'],
            'author_tweet_count': user_info['tweet_count'],
            'created_at': timestamp_str,
            'timestamp': timestamp_str,
            'date': created_at.strftime('%Y-%m-%d'),
            'time': created_at.strftime('%H:%M:%S'),
            'time_text': created_at.strftime('%I:%M %p · %b %d, %Y'),
            'metrics': metrics,
            'engagement_score': engagement_score,
            'likes': metrics['like_count'],
            'retweets': metrics['retweet_count'],
            'replies': metrics['reply_count'],
            'quotes': metrics['quote_count'],
            'bookmarks': metrics['bookmark_count'],
            'impressions': metrics['impression_count'],
            'hashtags': hashtags,
            'mentions': mentions,
            'urls': urls,
            'keyphrase': self._extract_keyphrase(tweet.get('text', '')),
            'word_count': len(tweet.get('text', '').split()),
            'char_count': len(tweet.get('text', '')),
            'has_media': bool(media_info['images'] or media_info['videos'] or media_info['gifs']),
            'media_count': len(media_info['images']) + len(media_info['videos']) + len(media_info['gifs']),
            'images': media_info['images'] or None,
            'videos': media_info['videos'] or None,
            'gifs': media_info['gifs'] or None,
            'keyword': keyword,
            'search_timestamp': datetime.now().isoformat(),
            'source_type': 'social_network',
            'source_domain': 'twitter.com',
            'platform': 'twitter',
            'content_type': 'tweet',
            'is_retweet': tweet.get('text', '').startswith('RT @'),
            'is_reply': bool(tweet.get('in_reply_to_user_id')),
            'is_quote': bool(tweet.get('referenced_tweets')),
            'has_hashtags': bool(hashtags),
            'has_mentions': bool(mentions),
            'has_urls': bool(urls)
        }
        return tweet_data

    def _save_batch(self, batch_data: List[Dict], batch_num: int):
        filename = f"tweets_batch_{batch_num}.json"
        for attempt in range(3):
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(batch_data, f, indent=2, ensure_ascii=False, default=str)
                logger.info(f"Saved batch {batch_num} with {len(batch_data)} tweets to {filename}")
                return
            except Exception as e:
                logger.error(f"Error saving batch {batch_num} (attempt {attempt + 1}): {e}")
                time.sleep(2)
        logger.error(f"Failed to save batch {batch_num} after 3 attempts")

    async def _check_rate_limit(self):
        """Check rate limit status before making a request"""
        if self.requests_remaining <= 0 and time.time() < self.rate_limit_reset:
            wait_time = self.rate_limit_reset - time.time()
            logger.info(f"Rate limit reached. Waiting {wait_time:.2f} seconds")
            await asyncio.sleep(wait_time)
            self.requests_remaining = 900
            self.rate_limit_reset = 0
        elif self.requests_remaining <= 0:
            logger.warning("Rate limit exhausted but reset time passed. Resetting limits.")
            self.requests_remaining = 900
            self.rate_limit_reset = 0

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError))
    )
    async def _fetch_tweets_async(self, query: str, start_time: datetime, next_token: Optional[str] = None) -> Dict:
        await self._init_session()
        await self._check_rate_limit()
        params = {
            'query': query,
            'max_results': self.max_results_per_request,
            'tweet.fields': 'created_at,text,public_metrics,entities,id,author_id,lang,possibly_sensitive,referenced_tweets',
            'user.fields': 'username,name,verified,public_metrics,profile_image_url',
            'expansions': 'author_id,attachments.media_keys',
            'media.fields': 'type,url,preview_image_url,width,height,duration_ms',
            'start_time': start_time.isoformat() + 'Z'
        }
        if next_token:
            params['next_token'] = next_token
        async with self.session.get('https://api.twitter.com/2/tweets/search/recent', params=params) as response:
            if response.status == 429:
                reset_time = int(response.headers.get('x-rate-limit-reset', time.time() + 900))
                self.rate_limit_reset = reset_time
                self.requests_remaining = 0
                wait_time = max(reset_time - time.time(), 1)
                logger.warning(f"Rate limit hit. Waiting {wait_time:.2f} seconds")
                await asyncio.sleep(wait_time)
                raise aiohttp.ClientError("Rate limit exceeded")
            if response.status == 403:
                logger.error(f"HTTP 403: Authentication error. Ensure your bearer token has access to /2/tweets/search/recent.")
                raise aiohttp.ClientError("Authentication error")
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"HTTP {response.status}: {error_text}")
                raise aiohttp.ClientResponseError(
                    response.request_info, response.history, status=response.status, message=error_text
                )
            data = await response.json()
            self.requests_remaining = int(response.headers.get('x-rate-limit-remaining', self.requests_remaining - 1))
            logger.debug(f"API response: {json.dumps(data, indent=2)}")
            return data

    async def scrape_tweets(self, keyword: str, days_back: int = 7) -> List[Dict]:
        # Correct common typo
        if keyword.lower() == 'miroladdodik':
            keyword = 'milorad dodik'
            logger.info(f"Corrected keyword to: '{keyword}'")
        logger.info(f"Starting scrape for keyword: '{keyword}' (last {days_back} days)")
        if not await self._check_rate_limit_status():
            logger.error("Cannot proceed with scraping due to rate limit or invalid token. Please check Twitter Developer Portal.")
            print("Error: Rate-limited or invalid bearer token. Regenerate token in Twitter Developer Portal or wait for rate limit reset.")
            return []
        query = f"{keyword}"  # Simplified query
        start_time = datetime.now() - timedelta(days=days_back)
        next_token = None
        batch_num = 0
        batch_data = []
        try:
            while len(self.tweets_data) < self.total_limit:
                try:
                    logger.info(f"Fetching tweets... Current count: {len(self.tweets_data)}")
                    response = await self._fetch_tweets_async(query, start_time, next_token)
                    if response.get('data'):
                        logger.info(f"Received {len(response['data'])} tweets in response")
                        with ThreadPoolExecutor(max_workers=3) as executor:
                            futures = [
                                executor.submit(self._process_tweet, tweet, response.get('includes'), keyword)
                                for tweet in response['data']
                            ]
                            for future in futures:
                                try:
                                    processed_tweet = future.result(timeout=10)
                                    if processed_tweet:
                                        self.tweets_data.append(processed_tweet)
                                        batch_data.append(processed_tweet)
                                        if len(batch_data) >= self.batch_size:
                                            self._save_batch(batch_data, batch_num)
                                            batch_data = []
                                            batch_num += 1
                                        if len(self.tweets_data) >= self.total_limit:
                                            break
                                except Exception as e:
                                    logger.error(f"Error processing tweet: {e}")
                        next_token = response.get('meta', {}).get('next_token')
                        if not next_token:
                            logger.info("No more tweets available")
                            break
                        await asyncio.sleep(1.1)  # Respect 900 req/15min
                    else:
                        logger.info(f"No tweets found for keyword '{keyword}'")
                        break
                except aiohttp.ClientError as e:
                    if "Rate limit" in str(e):
                        logger.error(f"Rate limit error persists: {e}")
                        print(f"Error: Rate limit exceeded. Wait until {datetime.fromtimestamp(self.rate_limit_reset)} or regenerate token.")
                        break
                    elif "Authentication" in str(e):
                        logger.error(f"Authentication error: {e}")
                        print("Error: Invalid bearer token. Regenerate token in Twitter Developer Portal.")
                        break
                    logger.error(f"API Error: {e}")
                    break
                except Exception as e:
                    logger.error(f"Unexpected error: {e}", exc_info=True)
                    break
            if batch_data:
                self._save_batch(batch_data, batch_num)
            if self.tweets_data:
                for attempt in range(3):
                    try:
                        with open(self.output_file, 'w', encoding='utf-8') as f:
                            json.dump(self.tweets_data, f, indent=2, ensure_ascii=False, default=str)
                        logger.info(f"Saved complete dataset: {len(self.tweets_data)} tweets to {self.output_file}")
                        break
                    except Exception as e:
                        logger.error(f"Error saving final dataset (attempt {attempt + 1}): {e}")
                        time.sleep(2)
            else:
                logger.warning("No tweets were collected.")
            return self.tweets_data
        except KeyboardInterrupt:
            logger.info("Script interrupted by user. Saving partial data.")
            if batch_data:
                self._save_batch(batch_data, batch_num)
            if self.tweets_data:
                with open(self.output_file, 'w', encoding='utf-8') as f:
                    json.dump(self.tweets_data, f, indent=2, ensure_ascii=False, default=str)
                logger.info(f"Saved partial dataset: {len(self.tweets_data)} tweets to {self.output_file}")
            raise
        finally:
            await self._close_session()

    def display_summary(self, tweets_data: List[Dict]):
        if not tweets_data:
            print("No tweets collected.")
            return
        print(f"\n{'='*50}")
        print(f"TWITTER SCRAPER SUMMARY")
        print(f"{'='*50}")
        print(f"Total tweets collected: {len(tweets_data)}")
        print(f"Unique users: {len(set(tweet['username'] for tweet in tweets_data if tweet['username']))}")
        print(f"Data saved to: {self.output_file}")
        total_engagement = sum(tweet['engagement_score'] for tweet in tweets_data)
        avg_engagement = total_engagement / len(tweets_data) if tweets_data else 0
        tweets_with_media = sum(1 for tweet in tweets_data if tweet['has_media'])
        tweets_with_hashtags = sum(1 for tweet in tweets_data if tweet['has_hashtags'])
        tweets_with_urls = sum(1 for tweet in tweets_data if tweet['has_urls'])
        print(f"\nENGAGEMENT STATISTICS:")
        print(f"Average engagement score: {avg_engagement:.2f}")
        print(f"Tweets with media: {tweets_with_media} ({tweets_with_media/len(tweets_data)*100:.1f}%)")
        print(f"Tweets with hashtags: {tweets_with_hashtags} ({tweets_with_hashtags/len(tweets_data)*100:.1f}%)")
        print(f"Tweets with URLs: {tweets_with_urls} ({tweets_with_urls/len(tweets_data)*100:.1f}%)")
        top_tweets = sorted(tweets_data, key=lambda x: x['engagement_score'], reverse=True)[:3]
        print(f"\nTOP PERFORMING TWEETS:")
        for i, tweet in enumerate(top_tweets, 1):
            print(f"\n{i}. @{tweet['username']} - Engagement: {tweet['engagement_score']}")
            print(f"   Created: {tweet['time_text']}")
            print(f"   Text: {tweet['text'][:100]}{'...' if len(tweet['text']) > 100 else ''}")
            print(f"   Metrics: {tweet['likes']} likes, {tweet['retweets']} retweets, {tweet['replies']} replies")

async def main(keyword: Optional[str] = None, days_back: Optional[int] = None):
    bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
    if not bearer_token:
        print("Error: TWITTER_BEARER_TOKEN environment variable not set.")
        print("Please ensure your .env file contains a valid Bearer Token.")
        return
    scraper = TwitterScraperOptimized(bearer_token)
    if keyword is None:
        try:
            keyword = input("Enter the keyword to search for tweets: ").strip()
        except EOFError:
            print("Input interrupted. Exiting.")
            return
    if not keyword:
        print("No keyword provided. Exiting.")
        return
    if days_back is None:
        try:
            days_input = input("Enter number of days to search back (default: 7): ").strip()
            days_back = int(days_input) if days_input else 7
        except EOFError:
            print("Input interrupted. Using default 7 days.")
            days_back = 7
        except ValueError:
            print("Invalid input. Using default 7 days.")
            days_back = 7
    print(f"\nStarting optimized Twitter scraper...")
    print(f"Keyword: '{keyword}'")
    print(f"Time range: Last {days_back} days")
    print(f"Target: {scraper.total_limit} tweets")
    print(f"Output: {scraper.output_file}")
    start_time = time.time()
    tweets_data = await scraper.scrape_tweets(keyword, days_back)
    end_time = time.time()
    print(f"\nScraping completed in {end_time - start_time:.2f} seconds")
    scraper.display_summary(tweets_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter Scraper")
    parser.add_argument('--keyword', type=str, help="Keyword to search for tweets")
    parser.add_argument('--days', type=int, help="Number of days to search back")
    args = parser.parse_args()
    asyncio.run(main(args.keyword, args.days))