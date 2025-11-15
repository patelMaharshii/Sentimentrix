import praw
import os
import pandas as pd
from dotenv import load_dotenv
import re

load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('CLIENT_SECRET'), 
    user_agent=os.getenv('USER_AGENT')
)

# Read subreddits from file
with open("reddit_threads.txt", "r") as f:
    subreddits = [line.strip() for line in f.readlines() if line.strip()]

# Create directory for csv files
output_dir = "./reddit_data/"
os.makedirs(output_dir, exist_ok=True)

def is_image_url(url):
    """Check if URL points to an image"""
    if not url:
        return False
    
    # Direct image extensions
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')
    if url.lower().endswith(image_extensions):
        return True
    
    # Reddit image hosting patterns
    reddit_image_patterns = [
        r'i\.redd\.it',           # i.redd.it/image.jpg
        r'preview\.redd\.it',     # preview.redd.it/image.jpg
        r'external-preview\.redd\.it'  # external preview images
    ]
    
    for pattern in reddit_image_patterns:
        if re.search(pattern, url):
            return True
    
    # Imgur patterns
    imgur_patterns = [
        r'i\.imgur\.com',         # i.imgur.com/image.jpg
        r'imgur\.com/\w+\.(jpg|jpeg|png|gif)'  # imgur.com/abc123.jpg
    ]
    
    for pattern in imgur_patterns:
        if re.search(pattern, url):
            return True
    
    return False

def extract_image_urls_from_text(text):
    """Extract image URLs from comment/post text"""
    if not text:
        return []
    
    # Pattern to match URLs in text
    url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
    urls = re.findall(url_pattern, text)
    
    # Filter for image URLs
    image_urls = [url for url in urls if is_image_url(url)]
    return image_urls

def get_post_images(submission):
    """Extract all image information from a post"""
    images = []
    
    # Check if post URL is an image
    if is_image_url(submission.url):
        images.append({
            'source': 'post_url',
            'url': submission.url,
            'type': 'direct_link'
        })
    
    # Check if post has a gallery
    if hasattr(submission, 'is_gallery') and submission.is_gallery:
        if hasattr(submission, 'media_metadata'):
            for media_id, media_info in submission.media_metadata.items():
                if 's' in media_info and 'u' in media_info['s']:
                    # Replace preview URL with full resolution
                    image_url = media_info['s']['u'].replace('preview.redd.it', 'i.redd.it')
                    images.append({
                        'source': 'gallery',
                        'url': image_url,
                        'type': 'reddit_gallery',
                        'media_id': media_id
                    })
    
    # Check post text for image URLs
    text_images = extract_image_urls_from_text(submission.selftext)
    for url in text_images:
        images.append({
            'source': 'post_text',
            'url': url,
            'type': 'embedded_link'
        })
    
    return images

def scrape_subreddit_praw(subreddit_name, limit=100, pages=5):
    """
    Enhanced scraper that captures images from posts and comments
    """
    subreddit = reddit.subreddit(subreddit_name)
    posts_data = []
    comments_data = []
    images_data = []  # New: separate image tracking
    
    print(f"Scraping subreddit: {subreddit_name}")
    
    post_count = 0
    for submission in subreddit.hot(limit=limit * pages):
        if post_count >= (limit * pages):
            break
            
        print(f"  Processing post {post_count + 1}: {submission.title[:50]}...")
        
        # Get images from this post
        post_images = get_post_images(submission)
        
        # Enhanced post data with image information
        post_data = {
            "subreddit": subreddit_name,
            "post_id": submission.id,
            "post_title": submission.title,
            "post_score": submission.score,
            "post_url": f"https://reddit.com{submission.permalink}",
            "post_content_url": submission.url,  # The actual content URL
            "post_text": submission.selftext,
            "timestamp": submission.created_utc,
            "post_upvote_ratio": submission.upvote_ratio,
            "post_ups": submission.ups,
            "post_total_awards_received": submission.total_awards_received,
            "post_link_flair_text": submission.link_flair_text,
            "post_author": str(submission.author) if submission.author else "[deleted]",
            "post_num_comments": submission.num_comments,
            "has_images": len(post_images) > 0,  # New: boolean flag
            "num_images": len(post_images),      # New: image count
            "is_gallery": hasattr(submission, 'is_gallery') and submission.is_gallery,
            "content_type": "image" if is_image_url(submission.url) else "text"
        }
        
        posts_data.append(post_data)
        
        # Store image data separately
        for i, img in enumerate(post_images):
            image_data = {
                "subreddit": subreddit_name,
                "post_id": submission.id,
                "image_index": i,
                "image_url": img['url'],
                "image_source": img['source'],
                "image_type": img['type'],
                "media_id": img.get('media_id', None)
            }
            images_data.append(image_data)
        
        # Get comments with image detection
        post_comments = scrape_comments_praw_enhanced(submission, subreddit_name, images_data, max_comments=5)
        comments_data.extend(post_comments)
        
        post_count += 1
    
    return posts_data, comments_data, images_data

def scrape_comments_praw_enhanced(submission, subreddit_name, images_data, max_comments=5):
    """
    Enhanced comment scraper that detects images in comments
    """
    comments_data = []
    
    submission.comments.replace_more(limit=0)
    
    top_level_count = 0
    processed_comments = set()
    
    for comment in submission.comments:
        if top_level_count >= max_comments:
            break
            
        if hasattr(comment, 'body'):
            comment_thread = []
            _collect_comment_thread_enhanced(comment, comment_thread, submission.id, images_data)
            
            for comment_data in comment_thread:
                if comment_data['comment_id'] not in processed_comments:
                    comment_data['subreddit'] = subreddit_name
                    comments_data.append(comment_data)
                    processed_comments.add(comment_data['comment_id'])
            
            top_level_count += 1
    
    return comments_data

def _collect_comment_thread_enhanced(comment, comment_list, post_id, images_data):
    """
    Enhanced comment collection with image detection
    """
    if not hasattr(comment, 'body'):
        return
    
    # Extract images from comment text
    comment_images = extract_image_urls_from_text(comment.body)
    
    # Enhanced comment data
    comment_data = {
        "post_id": post_id,
        "comment_id": comment.id,
        "comment_text": comment.body,
        "comment_score": comment.score,
        "comment_author": str(comment.author) if comment.author else "[deleted]",
        "comment_created_utc": comment.created_utc,
        "parent_id": comment.parent_id,
        "reply_to_id": comment.parent_id.split('_')[1] if comment.parent_id else None,
        "comment_sentiment": "N/A",
        "has_images": len(comment_images) > 0,  # New: image detection
        "num_images": len(comment_images),     # New: image count
        "image_urls": "|".join(comment_images) if comment_images else None  # New: pipe-separated URLs
    }
    
    comment_list.append(comment_data)
    
    # Store comment images in images_data
    for i, img_url in enumerate(comment_images):
        image_data = {
            "subreddit": "",  # Will be filled by caller
            "post_id": post_id,
            "comment_id": comment.id,
            "image_index": i,
            "image_url": img_url,
            "image_source": "comment_text",
            "image_type": "embedded_link",
            "media_id": None
        }
        images_data.append(image_data)
    
    # Recursively process replies
    for reply in comment.replies:
        if hasattr(reply, 'body'):
            _collect_comment_thread_enhanced(reply, comment_list, post_id, images_data)

# Main scraping loop with image support
for subreddit in subreddits:
    print(f"\n=== Processing subreddit: {subreddit} ===")
    
    try:
        # Enhanced scraping with image detection
        posts_data, comments_data, images_data = scrape_subreddit_praw(subreddit)
        
        # Create DataFrames
        posts_df = pd.DataFrame(posts_data)
        comments_df = pd.DataFrame(comments_data)
        images_df = pd.DataFrame(images_data)
        
        # Save posts to CSV
        posts_filename = os.path.join(output_dir, f"{subreddit}_posts.csv")
        posts_df.to_csv(posts_filename, index=False)
        print(f"  Saved {len(posts_data)} posts to: {posts_filename}")
        
        # Save comments to CSV
        comments_filename = os.path.join(output_dir, f"{subreddit}_comments.csv")
        comments_df.to_csv(comments_filename, index=False)
        print(f"  Saved {len(comments_data)} comments to: {comments_filename}")
        
        # Save images to CSV (NEW!)
        if len(images_data) > 0:
            images_filename = os.path.join(output_dir, f"{subreddit}_images.csv")
            images_df.to_csv(images_filename, index=False)
            print(f"  Saved {len(images_data)} image URLs to: {images_filename}")
        
        # Enhanced summary
        print(f"  Summary for {subreddit}:")
        print(f"    - Posts: {len(posts_data)}")
        print(f"    - Comments: {len(comments_data)}")
        print(f"    - Images found: {len(images_data)}")
        if len(posts_data) > 0:
            posts_with_images = sum(1 for p in posts_data if p['has_images'])
            print(f"    - Posts with images: {posts_with_images}")
        
    except Exception as e:
        print(f"  ERROR processing {subreddit}: {e}")
        continue

print(f"\n=== Scraping Complete ===")
print(f"Data saved in: {output_dir}")
