#!/usr/bin/env python3
"""
Wattpad Young Adult Crawler - API Version
=========================================
Crawl qua Wattpad API v4 thay vì HTML parsing.

Endpoint: https://www.wattpad.com/v4/search/stories
- 105,575 stories available
- Trả về JSON trực tiếp
- Rate limit friendly

Usage:
    python wattpad_api_crawler.py [--max-stories 1000] [--output FILE]
"""

import argparse
import logging
import random
import time
import requests
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

API_BASE = "https://www.wattpad.com/v4/search/stories"
OUTPUT_DIR = Path(__file__).parent / "output"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.wattpad.com/search/young%20adult",
}


def fetch_stories(session: requests.Session, query: str, offset: int, 
                  limit: int = 50, retry: int = 3) -> Optional[dict]:
    """Fetch stories từ API với retry."""
    params = {
        "query": query,
        "limit": limit,
        "offset": offset,
        "mature": "false",
        "fields": "stories(id,title,description,user(name),voteCount,readCount,commentCount,"
                  "completed,numParts,cover,url,tags,length,language(id),"
                  "lastPublishedPart(createDate)),total,nextUrl"
    }
    
    for attempt in range(retry):
        try:
            # Random delay để tránh rate limit
            time.sleep(random.uniform(1.5, 3.0))
            
            response = session.get(API_BASE, params=params, headers=HEADERS, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                wait = (attempt + 1) * 5
                logger.warning(f"Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                logger.warning(f"Status {response.status_code}, retry {attempt + 1}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error: {e}, retry {attempt + 1}")
            time.sleep(2 ** attempt)
    
    return None


def parse_stories(data: dict) -> list[dict]:
    """Parse API response thành list of stories."""
    stories = data.get('stories', [])
    result = []
    
    for s in stories:
        # Chỉ lấy completed stories
        if not s.get('completed', False):
            continue
            
        result.append({
            "URL": s.get('url', ''),
            "Title": s.get('title', 'Unknown'),
            "Reads": s.get('readCount', 0),
            "Votes": s.get('voteCount', 0),
            "Status": "Completed",
        })
    
    return result


def save_partial(stories: list, output_path: Path, count: int):
    """Save intermediate results."""
    if not stories:
        return
    df = pd.DataFrame(stories)
    df = df.sort_values("Reads", ascending=False).reset_index(drop=True)
    df.insert(0, "Rank", range(1, len(df) + 1))
    
    partial = output_path.with_name(f"{output_path.stem}_partial_{count}.xlsx")
    df.to_excel(partial, index=False)
    logger.info(f"  [Saved partial: {len(stories)} stories]")


def run_crawler(max_stories: int = 500, output_file: str = None, 
                 limit_per_request: int = 50):
    if output_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = str(OUTPUT_DIR / f"wattpad_young_adult_{timestamp}.xlsx")
    
    output_path = Path(output_file)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting - target: {max_stories} stories")
    logger.info(f"Output: {output_path}")
    
    session = requests.Session()
    
    all_stories = []
    offset = 0
    total_available = 0
    consecutive_empty = 0
    page = 0
    
    while len(all_stories) < max_stories:
        page += 1
        
        logger.info(f"Fetching page {page} (offset={offset})...")
        
        data = fetch_stories(session, "young adult", offset, limit_per_request)
        
        if not data:
            logger.error("Failed to fetch data, stopping")
            break
        
        # Get total if first request
        if total_available == 0:
            total_available = data.get('total', 0)
            logger.info(f"Total stories available: {total_available:,}")
        
        # Parse stories
        stories = parse_stories(data)
        new_count = len(stories)
        
        logger.info(f"  Got {new_count} stories")
        
        if new_count == 0:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                logger.info("No more stories, stopping")
                break
        else:
            consecutive_empty = 0
            all_stories.extend(stories)
        
        # Check if done
        if len(all_stories) >= max_stories:
            logger.info(f"Reached target {max_stories}!")
            break
        
        # Get next offset from response
        next_url = data.get('nextUrl', '')
        if next_url:
            # Extract offset from nextUrl
            import re
            offset_match = re.search(r'offset=(\d+)', next_url)
            if offset_match:
                offset = int(offset_match.group(1))
            else:
                offset += limit_per_request
        else:
            offset += limit_per_request
        
        # Save every 5 pages
        if page % 5 == 0:
            save_partial(all_stories, output_path, len(all_stories))
        
        logger.info(f"  Total collected: {len(all_stories)}")
    
    session.close()
    
    if not all_stories:
        logger.warning("No stories collected!")
        return None
    
    # Trim to max_stories
    all_stories = all_stories[:max_stories]
    
    # Final save
    df = pd.DataFrame(all_stories)
    df = df.sort_values("Reads", ascending=False).reset_index(drop=True)
    df.insert(0, "Rank", range(1, len(df) + 1))
    df.to_excel(output_path, index=False)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"DONE! Total: {len(all_stories)} stories from {page} pages")
    logger.info(f"File: {output_path}")
    logger.info(f"{'='*60}")
    
    if not df.empty:
        status_counts = df['Status'].value_counts().to_dict()
        logger.info(f"\nStats:")
        logger.info(f"  Total Reads: {df['Reads'].sum():,}")
        logger.info(f"  Total Votes: {df['Votes'].sum():,}")
        logger.info(f"  Status: {status_counts}")
        
        logger.info(f"\nTop 10 by Reads:")
        for _, row in df.head(10).iterrows():
            reads_str = f"{row['Reads']:,}"
            votes_str = f"{row['Votes']:,}"
            logger.info(f"  {row['Rank']}. {row['Title'][:50]}... | {reads_str} reads | {votes_str} votes")
    
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Wattpad API Crawler")
    parser.add_argument("--max-stories", type=int, default=500)
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--limit", type=int, default=50, help="Stories per request (max 50)")
    args = parser.parse_args()
    
    run_crawler(
        max_stories=args.max_stories,
        output_file=args.output,
        limit_per_request=min(args.limit, 50),
    )


if __name__ == "__main__":
    main()
