#!/usr/bin/env python3
"""
Wattpad Young Adult Crawler v4
===============================
Crawl truyện từ Wattpad search "young adult".

Strategy:
- Scroll và click "Load more" button
- Save data thường xuyên để tránh mất khi crash
- Extract: URL, Title, Reads, Votes, Status

Usage:
    python wattpad_crawler.py [--max-stories 500] [--output FILE]
"""

import argparse
import logging
import random
import re
import time
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.wattpad.com/search/young%20adult"
OUTPUT_DIR = Path(__file__).parent / "output"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


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
    
    # Xóa phần mô tả trùng lặp
    parts = title.split()
    if len(parts) > 15:
        half = len(parts) // 2
        first_half = ' '.join(parts[:half])
        second_half = ' '.join(parts[half:])
        if first_half == second_half:
            title = first_half
    
    # Xóa stats
    title = re.sub(r'\s*[\d,.]+\s*[kK]?\s*[Rr]eads?', '', title)
    title = re.sub(r'\s*[\d,.]+\s*[kK]?\s*[Vv]otes?', '', title)
    title = re.sub(r'\s*[✓✔]\s*', ' ', title)
    title = re.sub(r'\s*(Complete|Completed|Ongoing|In Progress)\s*', ' ', title, flags=re.IGNORECASE)
    
    # Cắt sau ***
    if '***' in title:
        title = title.split('***')[0].strip()
    
    # Trim
    title = re.sub(r'\s+', ' ', title).strip()
    if len(title) > 200:
        title = title[:200] + "..."
    
    return title or "Unknown"


def extract_stories(page) -> list[dict]:
    """Extract stories từ page hiện tại."""
    stories = []
    seen_urls = set()
    
    items = page.query_selector_all('li.list-group-item')
    
    for item in items:
        link = item.query_selector('a[href^="/story/"]')
        if not link:
            continue
        
        href = link.get_attribute('href')
        if not href or href in seen_urls:
            continue
        seen_urls.add(href)
        
        url = f"https://www.wattpad.com{href}"
        title = clean_title(link.text_content())
        
        # Parse stats từ spans
        spans = item.query_selector_all('span')
        all_texts = [span.text_content() for span in spans]
        
        reads = 0
        votes = 0
        status = "Unknown"
        
        for text in all_texts:
            text_lower = text.strip().lower()
            
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


def save_partial(stories: list, output_path: Path, count: int):
    """Save intermediate results."""
    if not stories:
        return
    df = pd.DataFrame(stories)
    df = df.sort_values("Reads", ascending=False).reset_index(drop=True)
    df.insert(0, "Rank", range(1, len(df) + 1))
    
    # Save partial
    partial = output_path.with_name(f"{output_path.stem}_partial_{count}{output_path.suffix}")
    df.to_excel(partial, sheet_name="Wattpad Young Adult", index=False)
    logger.info(f"  [Saved partial: {len(stories)} stories]")


def run_crawler(max_stories: int = 500, output_file: str = None, headless: bool = True):
    if output_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = str(OUTPUT_DIR / f"wattpad_young_adult_{timestamp}.xlsx")
    
    output_path = Path(output_file)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting - target: {max_stories} stories")
    logger.info(f"Output: {output_path}")
    
    all_stories = []
    seen_urls = set()
    scroll_count = 0
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            
            page = context.new_page()
            
            logger.info("Navigating...")
            page.goto(BASE_URL, wait_until="networkidle", timeout=120000)
            time.sleep(3)
            
            story_count = page.evaluate("document.querySelectorAll('a[href^=\"/story/\"]').length")
            logger.info(f"Page loaded, found {story_count} links")
            
            while len(all_stories) < max_stories:
                scroll_count += 1
                
                # Extract stories
                stories = extract_stories(page)
                
                new_count = 0
                for story in stories:
                    if story["URL"] not in seen_urls:
                        seen_urls.add(story["URL"])
                        all_stories.append(story)
                        new_count += 1
                
                total = len(all_stories)
                logger.info(f"Scroll #{scroll_count}: {total} stories ({new_count} new)")
                
                # Save every 2 scrolls
                if scroll_count % 2 == 0:
                    save_partial(all_stories, output_path, total)
                
                # Check if done
                if total >= max_stories:
                    logger.info(f"Reached target {max_stories}!")
                    break
                
                if new_count == 0:
                    consecutive_empty = (consecutive_empty if 'consecutive_empty' in dir() else 0) + 1
                    if consecutive_empty >= 5:
                        logger.info("No new stories for 5 scrolls, stopping")
                        break
                else:
                    consecutive_empty = 0
                
                # Scroll
                page.evaluate("window.scrollBy(0, window.innerHeight * 1.5)")
                time.sleep(1.5)
                
                # Click load more
                try:
                    btn = page.query_selector('button:has-text("Load more")')
                    if btn and btn.is_visible():
                        btn.click()
                        time.sleep(1.5)
                except:
                    pass
            
            browser.close()
            
    except Exception as e:
        logger.error(f"Error: {e}")
    
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
    logger.info(f"DONE! Total: {len(all_stories)} stories")
    logger.info(f"File: {output_path}")
    logger.info(f"{'='*60}")
    
    if not df.empty:
        logger.info(f"\nTop 10 by Reads:")
        for _, row in df.head(10).iterrows():
            reads_str = f"{row['Reads']:,}" if row['Reads'] else "N/A"
            votes_str = f"{row['Votes']:,}" if row['Votes'] else "N/A"
            logger.info(f"  {row['Rank']}. {row['Title'][:50]}... | {reads_str} reads | {votes_str} votes")
    
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Wattpad Crawler v4")
    parser.add_argument("--max-stories", type=int, default=500)
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--visible", action="store_true")
    args = parser.parse_args()
    
    run_crawler(
        max_stories=args.max_stories,
        output_file=args.output,
        headless=not args.visible,
    )


if __name__ == "__main__":
    main()
