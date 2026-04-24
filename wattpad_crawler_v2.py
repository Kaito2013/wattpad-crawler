#!/usr/bin/env python3
"""
Wattpad Young Adult Crawler - Requests Version
==============================================
Dùng requests thay vì Playwright để tránh memory crash.

Strategy:
- Request trực tiếp đến Wattpad search page
- Parse HTML với BeautifulSoup
- Respect rate limit: delay giữa requests
- Retry với exponential backoff khi bị block

Usage:
    python wattpad_crawler_v2.py [--max-stories 500] [--output FILE]
"""

import argparse
import logging
import random
import re
import time
import requests
from datetime import datetime
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.wattpad.com/search/young%20adult"
API_URL = "https://www.wattpad.com/api/v3/stories"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

OUTPUT_DIR = Path(__file__).parent / "output"


def parse_number(text: str) -> int:
    if not text:
        return 0
    text = text.strip().lower().replace(",", "").replace(" ", "")
    match = re.search(r'([\d.]+)\s*([kmb])?', text)
    if not match:
        return 0
    num_str = match.group(1)
    if not num_str or num_str == '.':
        return 0
    try:
        number = float(num_str)
    except ValueError:
        return 0
    suffix = match.group(2)
    if suffix == 'k':
        number *= 1_000
    elif suffix == 'm':
        number *= 1_000_000
    elif suffix == 'b':
        number *= 1_000_000_000
    return int(number)


def clean_title(title: str) -> str:
    if not title:
        return "Unknown"
    title = title.strip()
    
    # Xóa phần trùng lặp
    parts = title.split()
    if len(parts) > 15:
        half = len(parts) // 2
        if ' '.join(parts[:half]) == ' '.join(parts[half:]):
            title = ' '.join(parts[:half])
    
    # Xóa stats
    title = re.sub(r'\s*[\d,.]+\s*[kK]?\s*[Rr]eads?', '', title)
    title = re.sub(r'\s*[\d,.]+\s*[kK]?\s*[Vv]otes?', '', title)
    title = re.sub(r'\s*[✓✔]\s*', ' ', title)
    title = re.sub(r'\s*(Complete|Completed|Ongoing)\s*', ' ', title, flags=re.IGNORECASE)
    
    if '***' in title:
        title = title.split('***')[0].strip()
    
    title = re.sub(r'\s+', ' ', title).strip()
    if len(title) > 200:
        title = title[:200] + "..."
    
    return title or "Unknown"


def fetch_page(session: requests.Session, url: str, retry: int = 3) -> Optional[BeautifulSoup]:
    """Fetch URL với retry và exponential backoff."""
    for attempt in range(retry):
        try:
            # Random delay để tránh detection
            time.sleep(random.uniform(1.0, 2.5))
            
            response = session.get(url, headers=HEADERS, timeout=30)
            
            if response.status_code == 200:
                return BeautifulSoup(response.text, 'html.parser')
            elif response.status_code == 429:
                # Rate limited - wait longer
                wait_time = (attempt + 1) * 10
                logger.warning(f"Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.warning(f"Status {response.status_code}, retry {attempt + 1}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error: {e}, retry {attempt + 1}")
            time.sleep(2 ** attempt)
    
    return None


def extract_stories_from_page(soup: BeautifulSoup) -> list[dict]:
    """Extract stories từ HTML page."""
    stories = []
    seen_urls = set()
    
    items = soup.select('li.list-group-item')
    
    for item in items:
        link = item.select_one('a[href^="/story/"]')
        if not link:
            continue
        
        href = link.get('href', '')
        if not href or href in seen_urls:
            continue
        seen_urls.add(href)
        
        url = f"https://www.wattpad.com{href}"
        title = clean_title(link.get_text())
        
        # Parse stats
        spans = item.select('span')
        reads = 0
        votes = 0
        status = "Unknown"
        
        for span in spans:
            text = span.get_text()
            text_lower = text.lower().strip()
            
            if text_lower.startswith('reads '):
                val_str = text.strip().split(' ', 1)[1]
                reads = parse_number(val_str)
            elif text_lower.startswith('votes '):
                val_str = text.strip().split(' ', 1)[1]
                votes = parse_number(val_str)
            
            if 'complete' in text_lower:
                status = "Completed"
            elif 'ongoing' in text_lower:
                status = "Ongoing"
        
        stories.append({
            "URL": url,
            "Title": title,
            "Reads": reads,
            "Votes": votes,
            "Status": status
        })
    
    return stories


def extract_next_page_url(soup: BeautifulSoup) -> Optional[str]:
    """Extract "Load more" URL từ page."""
    # Wattpad dùng data-action cho load more
    load_more = soup.select_one('button[data-action="load-more"], a[href*="?page="]')
    if load_more:
        href = load_more.get('href') or load_more.get('data-url')
        if href:
            if href.startswith('/'):
                return f"https://www.wattpad.com{href}"
            return href
    
    # Thử tìm next page link
    next_link = soup.select_one('a[rel="next"]')
    if next_link:
        href = next_link.get('href', '')
        if href.startswith('/'):
            return f"https://www.wattpad.com{href}"
        return href
    
    return None


def save_partial(stories: list, output_path: Path, total: int):
    """Save intermediate results."""
    if not stories:
        return
    df = pd.DataFrame(stories)
    df = df.sort_values("Reads", ascending=False).reset_index(drop=True)
    df.insert(0, "Rank", range(1, len(df) + 1))
    
    partial = output_path.with_name(f"{output_path.stem}_partial_{total}{output_path.suffix}")
    df.to_excel(partial, sheet_name="Wattpad Young Adult", index=False)
    logger.info(f"  [Saved partial: {len(stories)} stories]")


def run_crawler(max_stories: int = 500, output_file: str = None):
    if output_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = str(OUTPUT_DIR / f"wattpad_young_adult_{timestamp}.xlsx")
    
    output_path = Path(output_file)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting - target: {max_stories} stories")
    logger.info(f"Output: {output_path}")
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    all_stories = []
    seen_urls = set()
    page_count = 0
    current_url = BASE_URL
    
    while len(all_stories) < max_stories:
        page_count += 1
        
        logger.info(f"Fetching page {page_count}: {current_url[:60]}...")
        
        soup = fetch_page(session, current_url)
        if not soup:
            logger.error("Failed to fetch page, stopping")
            break
        
        # Extract stories
        stories = extract_stories_from_page(soup)
        
        new_count = 0
        for story in stories:
            if story["URL"] not in seen_urls:
                seen_urls.add(story["URL"])
                all_stories.append(story)
                new_count += 1
        
        total = len(all_stories)
        logger.info(f"  Found {len(stories)} stories, {new_count} new, total: {total}")
        
        # Save every 3 pages
        if page_count % 3 == 0:
            save_partial(all_stories, output_path, total)
        
        # Check done
        if total >= max_stories:
            logger.info(f"Reached target {max_stories}!")
            break
        
        # Get next page URL
        next_url = extract_next_page_url(soup)
        if not next_url:
            # Thử construct page URL
            if "?page=" in current_url:
                page_num = int(current_url.split("?page=")[1].split("&")[0]) + 1
            else:
                page_num = 2
            next_url = f"{BASE_URL}?page={page_num}"
            
            # Nếu không có stories mới sau 2 pages, có thể hết rồi
            if new_count == 0:
                consecutive_empty = getattr(run_crawler, 'consecutive_empty', 0) + 1
                if consecutive_empty >= 2:
                    logger.info("No new stories, assuming end of results")
                    break
                run_crawler.consecutive_empty = consecutive_empty
        
        current_url = next_url
        
        # Respect rate limit
        delay = random.uniform(2.0, 4.0)
        logger.info(f"  Waiting {delay:.1f}s before next request...")
        time.sleep(delay)
    
    session.close()
    
    if not all_stories:
        logger.warning("No stories collected!")
        return None
    
    # Final save
    df = pd.DataFrame(all_stories)
    df = df.sort_values("Reads", ascending=False).reset_index(drop=True)
    df.insert(0, "Rank", range(1, len(df) + 1))
    df.to_excel(output_path, sheet_name="Wattpad Young Adult", index=False)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"DONE! Total: {len(all_stories)} stories from {page_count} pages")
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
            reads_str = f"{row['Reads']:,}" if row['Reads'] else "N/A"
            votes_str = f"{row['Votes']:,}" if row['Votes'] else "N/A"
            logger.info(f"  {row['Rank']}. {row['Title'][:50]}... | {reads_str} reads | {votes_str} votes")
    
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Wattpad Crawler - Requests Version")
    parser.add_argument("--max-stories", type=int, default=500)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()
    
    run_crawler(
        max_stories=args.max_stories,
        output_file=args.output,
    )


if __name__ == "__main__":
    main()
