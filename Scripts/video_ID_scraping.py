# Artemis Tack, Iowa State University
# August 2025
#
# This script reads a CSV file containing from Google Political Ads Transparency, containing the Video ID, the 
# Creative ID and Advertiser ID, and then scraped Google's ad transparency website for the video's YouTube ID,
# it then writes the Creative ID, Advertiser ID and Video ID to a new CSV file.
#
#            ____                      ,
#           /---.'.__             ____//
#                '--.\           /.---'
#           _______  \\         //
#         /.------.\  \|      .'/  ______
#        //  ___  \ \ ||/|\  //  _/_----.\__
#       |/  /.-.\  \ \:|< >|// _/.'..\   '--'
#          //   \'. | \'.|.'/ /_/ /  \\
#         //     \ \_\/" ' ~\-'.-'    \\
#        //       '-._| :H: |'-.__     \\
#       //           (/'==='\)'-._\     ||
#       ||                        \\    \|
#       ||                         \\    '
# snd   |/                          \\
#                                    ||
#                                    ||
#                                    \\
#  

import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import time
import csv
import os
import json
from pathlib import Path

async def create_webkit_browser(playwright):
    """Create a single WebKit browser with optimized settings"""
    browser = await playwright.webkit.launch(
        headless=True,
        args=[
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor",
            "--disable-background-networking",
            "--disable-default-apps",
            "--disable-extensions"
        ]
    )
    return browser

async def create_optimized_context(browser):
    """Create an optimized browser context"""
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/17.2 Safari/537.36",
        viewport={'width': 1280, 'height': 720},
        bypass_csp=True,
        java_script_enabled=True,
    )
    
    # Block unnecessary resources
    await context.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2,mp4,webm,avi,mov}", 
                       lambda route: route.abort())
    
    return context

async def extract_video_id_with_page(page, cr, ar):
    """Extract video ID using Playwright page"""
    try:
        adtransparency_url = f"https://adstransparency.google.com/advertiser/{ar}/creative/{cr}?authuser=0&region=US&topic=political"
        
        await page.goto(adtransparency_url, wait_until='domcontentloaded', timeout=20000)
        
        # Wait for the fletch-render iframe
        fletch_render_iframe = await page.wait_for_selector('iframe[id^="fletch-render"]', timeout=2500)
        fletch_frame = await fletch_render_iframe.content_frame()
        if not fletch_frame:
            return None
        
        # Wait for the google ad iframe
        google_ad_iframe = await fletch_frame.wait_for_selector('iframe[id^="google_ad"]', timeout=500)
        google_ad_frame = await google_ad_iframe.content_frame()
        if not google_ad_frame:
            return None
        
        # Wait for the video iframe
        video_iframe = await google_ad_frame.wait_for_selector('iframe[id^="video"]', timeout=500)
        video_iframe_src = await video_iframe.get_attribute('src')
        
        if video_iframe_src and "youtube.com/embed/" in video_iframe_src:
            video_id = video_iframe_src.split("youtube.com/embed/")[1].split("?")[0]
            return video_id
        
        return None
        
    except PlaywrightTimeoutError:
        return None
    except Exception as e:
        return None

class ProgressTracker:
    def __init__(self, progress_file):
        self.progress_file = progress_file
        self.processed_urls = set()
        self.results = []
        self.load_progress()
    
    def load_progress(self):
        """Load existing progress from file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    self.processed_urls = set(data.get('processed_urls', []))
                    self.results = data.get('results', [])
                print(f"Resumed: {len(self.processed_urls)} URLs already processed")
            except:
                print("Starting fresh (couldn't load progress file)")
    
    def save_progress(self):
        """Save current progress to file"""
        data = {
            'processed_urls': list(self.processed_urls),
            'results': self.results
        }
        with open(self.progress_file, 'w') as f:
            json.dump(data, f)
    
    def add_result(self, cr, ar, video_id):
        """Add a result and mark URL as processed"""
        url_key = f"{cr}_{ar}"
        if url_key not in self.processed_urls:
            self.processed_urls.add(url_key)
            if video_id:
                self.results.append({'cr': cr, 'ar': ar, 'video_id': video_id})
    
    def is_processed(self, cr, ar):
        """Check if URL was already processed"""
        return f"{cr}_{ar}" in self.processed_urls

async def process_url_batch_with_progress(browser, url_batch, batch_id, progress_tracker):
    """Process a batch of URLs with progress tracking"""
    context = await create_optimized_context(browser)
    batch_results = []
    
    try:
        for i, (cr, ar) in enumerate(url_batch):
            # Skip if already processed
            if progress_tracker.is_processed(cr, ar):
                print(f"Batch {batch_id}: SKIPPED - {cr} (already processed)")
                continue
            
            page = None
            try:
                page = await context.new_page()
                
                print(f"Batch {batch_id}: Processing {cr} ({i+1}/{len(url_batch)})")
                start_time = time.time()
                
                video_id = await extract_video_id_with_page(page, cr, ar)
                elapsed = time.time() - start_time
                
                # Add to progress tracker
                progress_tracker.add_result(cr, ar, video_id)
                
                if video_id:
                    print(f"Batch {batch_id}: SUCCESS - {cr} -> {video_id} ({elapsed:.2f}s)")
                    batch_results.append({'cr': cr, 'ar': ar, 'video_id': video_id})
                else:
                    print(f"Batch {batch_id}: FAILED - {cr} ({elapsed:.2f}s)")
                
                # Add small delay to be respectful to the server
                await asyncio.sleep(0.25)
                
            except Exception as e:
                print(f"Batch {batch_id}: ERROR - {cr}: {str(e)}")
                progress_tracker.add_result(cr, ar, None)
            finally:
                if page:
                    await page.close()
                    
    except Exception as e:
        print(f"Batch {batch_id}: Context error: {str(e)}")
    finally:
        await context.close()
    
    return batch_results

async def main():
    # Get input file name
    file_output_name = input("Enter the CSV file name (without extension): ")
    input_file = f'/Users/starlight/Documents/Accademia/Timing of negative ads/google-political-ads-transparency-bundle (1)/{file_output_name}.csv'
    
    # Progress tracking
    progress_file = f'/Users/starlight/Documents/Accademia/Timing of negative ads/google-political-ads-transparency-bundle (1)/progress_{file_output_name}.json'
    progress_tracker = ProgressTracker(progress_file)
    
    # Read URLs
    urls_to_process = []
    with open(input_file, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            cr = row[0]
            ar = row[1]
            urls_to_process.append((cr, ar))
    
    print(f"Total URLs to process: {len(urls_to_process)}")
    print(f"Already processed: {len(progress_tracker.processed_urls)}")
    print(f"Remaining: {len(urls_to_process) - len(progress_tracker.processed_urls)}")
    
    # Conservative settings for 8,000 URLs
    batch_size = 8          # Larger batches for efficiency
    num_concurrent_batches = 7  # Conservative concurrency (24 total concurrent)
    save_interval = 50      # Save progress every 50 URLs
    
    # Split URLs into batches
    url_batches = []
    for i in range(0, len(urls_to_process), batch_size):
        batch = urls_to_process[i:i + batch_size]
        url_batches.append(batch)
    
    print(f"Created {len(url_batches)} batches of {batch_size} URLs each")
    print(f"Running {num_concurrent_batches} batches concurrently")
    
    start_time = time.time()
    processed_count = len(progress_tracker.processed_urls)
    
    async with async_playwright() as playwright:
        browser = await create_webkit_browser(playwright)
        
        try:
            # Process batches in groups
            for i in range(0, len(url_batches), num_concurrent_batches):
                batch_group = url_batches[i:i + num_concurrent_batches]
                
                group_num = i//num_concurrent_batches + 1
                total_groups = (len(url_batches) + num_concurrent_batches - 1)//num_concurrent_batches
                print(f"\n=== Processing batch group {group_num}/{total_groups} ===")
                
                # Run batches in parallel
                tasks = []
                for j, batch in enumerate(batch_group):
                    batch_id = i + j + 1
                    task = process_url_batch_with_progress(browser, batch, batch_id, progress_tracker)
                    tasks.append(task)
                
                # Wait for all batches to complete
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Update processed count
                new_processed_count = len(progress_tracker.processed_urls)
                newly_processed = new_processed_count - processed_count
                processed_count = new_processed_count
                
                print(f"Group {group_num} complete: +{newly_processed} URLs processed")
                print(f"Total progress: {processed_count}/{len(urls_to_process)} ({processed_count/len(urls_to_process)*100:.1f}%)")
                
                # Save progress periodically
                if processed_count % save_interval == 0:
                    progress_tracker.save_progress()
                    print("Progress saved")
        
        finally:
            await browser.close()
    
    # Final save
    progress_tracker.save_progress()
    
    # Final statistics
    total_time = time.time() - start_time
    unique_results = {r['video_id']: r for r in progress_tracker.results}.values()
    
    print(f"\n=== FINAL RESULTS ===")
    print(f"Total time: {total_time/3600:.2f} hours")
    print(f"URLs processed: {len(progress_tracker.processed_urls)}")
    print(f"Videos found: {len(progress_tracker.results)}")
    print(f"Unique videos: {len(unique_results)}")
    print(f"Success rate: {len(progress_tracker.results)/len(progress_tracker.processed_urls)*100:.1f}%")
    
    # Save results to CSV
    output_file = f'/Users/starlight/Documents/Accademia/Timing of negative ads/google-political-ads-transparency-bundle (1)/video_ids_{file_output_name}.csv'
    
    with open(output_file, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Creative_ID', 'Advertiser_ID', 'Video_ID'])
        for result in progress_tracker.results:
            writer.writerow([result['cr'], result['ar'], result['video_id']])
    
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    asyncio.run(main())