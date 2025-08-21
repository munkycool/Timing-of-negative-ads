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
import threading
import time
import csv
import os
import json
import random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

class BrowserPool:
    """Thread-safe browser pool for concurrent processing"""
    def __init__(self, pool_size=3, browser_type='firefox'):
        self.pool_size = pool_size
        self.browser_type = browser_type
        self.browsers = Queue()
        self._lock = threading.Lock()
        
        # Proxy rotation setup
        self.proxies = [
            # Add your proxy list here - format: "ip:port" or "username:password@ip:port"
            # Example proxies (replace with your actual proxies):
            # "192.168.1.100:8080",
            # "192.168.1.101:8080",
            # "user:pass@192.168.1.102:8080",
            # "proxy1.example.com:3128",
            # "proxy2.example.com:3128"
        ]
        
        # If no proxies provided, use direct connection
        if not self.proxies:
            print("⚠️  No proxies configured - using direct connection")
            self.proxies = [None]  # None means no proxy
        else:
            print(f"✅ Configured {len(self.proxies)} proxies for rotation")
        
        self.proxy_index = 0
        self._initialize_browsers()
    
    def _get_next_proxy(self):
        """Get next proxy in rotation"""
        with self._lock:
            proxy = self.proxies[self.proxy_index]
            self.proxy_index = (self.proxy_index + 1) % len(self.proxies)
            return proxy
    
    def _create_browser(self):
        """Create a single browser instance with optimized settings"""
        
        # Get next proxy in rotation
        proxy = self._get_next_proxy()
        
        # Rotate user agents to avoid detection
        user_agents = [
            "Mozilla/5.0 (X11; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0",
            "Mozilla/5.0 (X11; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0", 
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
        ]
        
        # Firefox
        options = FirefoxOptions()
        options.add_argument("--headless")
        
        # Configure proxy if available
        if proxy:
            print(f"🌐 Using proxy: {proxy}")
            if "@" in proxy:
                # Proxy with authentication: username:password@ip:port
                auth_part, proxy_part = proxy.split("@")
                username, password = auth_part.split(":")
                ip, port = proxy_part.split(":")
                
                # Set proxy preferences for Firefox
                options.set_preference("network.proxy.type", 1)  # Manual proxy
                options.set_preference("network.proxy.http", ip)
                options.set_preference("network.proxy.http_port", int(port))
                options.set_preference("network.proxy.ssl", ip)
                options.set_preference("network.proxy.ssl_port", int(port))
                
                # Authentication (requires additional setup)
                options.set_preference("network.proxy.share_proxy_settings", True)
                
            else:
                # Simple proxy: ip:port
                ip, port = proxy.split(":")
                options.set_preference("network.proxy.type", 1)  # Manual proxy
                options.set_preference("network.proxy.http", ip)
                options.set_preference("network.proxy.http_port", int(port))
                options.set_preference("network.proxy.ssl", ip)
                options.set_preference("network.proxy.ssl_port", int(port))
                options.set_preference("network.proxy.share_proxy_settings", True)
        
        # Performance optimizations
        options.set_preference("dom.webdriver.enabled", False)
        options.set_preference("useAutomationExtension", False)
        options.set_preference("general.useragent.override", random.choice(user_agents))
        
        # Disable images for faster loading (but keep JavaScript enabled)
        options.set_preference("permissions.default.image", 2)
        # options.set_preference("javascript.enabled", False)  # COMMENTED OUT - we need JS!
        
        # Additional performance settings
        options.set_preference("dom.ipc.plugins.enabled.libflashplayer.so", False)
        options.set_preference("media.volume_scale", "0.0")
        
        # Custom Firefox binary path
        firefox_binary_path = "/home/uly/Timing-of-negative-ads/data/firefox/firefox/firefox"
        options.binary_location = firefox_binary_path
        
        # Custom geckodriver path (for older Selenium versions)
        geckodriver_path = "/home/uly/Timing-of-negative-ads/data/firefox/geckodriver"
        
        driver = webdriver.Firefox(executable_path=geckodriver_path, options=options)
        
        return driver
    
    def _initialize_browsers(self):
        """Initialize the browser pool"""
        for _ in range(self.pool_size):
            try:
                browser = self._create_browser()
                self.browsers.put(browser)
            except Exception as e:
                print(f"Error creating browser: {e}")
    
    def get_browser(self, timeout=30):
        """Get a browser from the pool"""
        try:
            return self.browsers.get(timeout=timeout)
        except:
            # If pool is empty, create a new browser
            return self._create_browser()
    
    def return_browser(self, browser):
        """Return a browser to the pool"""
        try:
            self.browsers.put_nowait(browser)
        except:
            # If queue is full, properly close the browser
            try:
                browser.close()  # Close the current window
            except:
                pass
            try:
                browser.quit()   # Quit the driver
            except:
                pass
    
    def close_all(self):
        """Close all browsers in the pool"""
        while not self.browsers.empty():
            try:
                browser = self.browsers.get_nowait()
                # Properly close browser windows first, then quit the driver
                try:
                    browser.close()  # Close the current window
                except:
                    pass
                try:
                    browser.quit()   # Quit the driver and close all windows
                except:
                    pass
            except:
                pass
    
    def add_proxies(self, proxy_list):
        """Add a list of proxies to the rotation"""
        with self._lock:
            self.proxies = proxy_list if proxy_list else [None]
            self.proxy_index = 0
            print(f"✅ Updated proxy list: {len(self.proxies)} proxies")

def extract_video_id_with_selenium(driver, cr, ar):
    """Extract video ID using Selenium WebDriver"""
    try:
        adtransparency_url = f"https://adstransparency.google.com/advertiser/{ar}/creative/{cr}"
        
        driver.get(adtransparency_url)
        
        # Check for rate limiting indicators
        page_source = driver.page_source.lower()
        page_title = driver.title.lower()
        
        # Common rate limiting/blocking indicators
        rate_limit_indicators = [
            "rate limit",
            "too many requests", 
            "429",
            "blocked",
            "captcha",
            "unusual traffic",
            "suspicious activity",
            "access denied",
            "forbidden",
            "503 service unavailable",
            "502 bad gateway",
            "cloudflare"
        ]
        
        # Check if we're being rate limited
        for indicator in rate_limit_indicators:
            if indicator in page_source or indicator in page_title:
                raise Exception(f"RATE LIMITED: Detected '{indicator}' on page for {cr}")
        
        # Check for empty or error pages
        if len(page_source) < 1000:  # Suspiciously small page
            raise Exception(f"SUSPICIOUS: Very small page response ({len(page_source)} chars) for {cr}")
        
        wait = WebDriverWait(driver, 20)  # Increased timeout
        
        # Wait for the fletch-render iframe
        fletch_render_iframe = WebDriverWait(driver, 7).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'iframe[id^="fletch-render"]'))
        )
        
        # Switch to the fletch-render iframe
        driver.switch_to.frame(fletch_render_iframe)
        
        # Wait for the google ad iframe with reasonable timeout
        google_ad_wait = WebDriverWait(driver, 5)  # Increased timeout
        google_ad_iframe = google_ad_wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'iframe[id^="google_ad"]'))
        )
        
        # Switch to the google ad iframe
        driver.switch_to.frame(google_ad_iframe)
        
        # Wait for the video iframe with reasonable timeout
        video_wait = WebDriverWait(driver, 5)  # Increased timeout
        video_iframe = video_wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'iframe[id^="video"]'))
        )
        
        # Get the video iframe source
        video_iframe_src = video_iframe.get_attribute('src')
        
        # Switch back to default content
        driver.switch_to.default_content()
        
        if video_iframe_src and "youtube.com/embed/" in video_iframe_src:
            video_id = video_iframe_src.split("youtube.com/embed/")[1].split("?")[0]
            return video_id
        
        return None
        
    except TimeoutException:
        driver.switch_to.default_content()
        return None
    except Exception as e:
        driver.switch_to.default_content()
        # Re-raise the exception so it can be handled properly in process_single_url
        raise e

class ProgressTracker:
    def __init__(self, progress_file):
        self.progress_file = progress_file
        self.processed_urls = set()
        self.results = []
        self._lock = threading.Lock()
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
        with self._lock:
            data = {
                'processed_urls': list(self.processed_urls),
                'results': self.results
            }
            with open(self.progress_file, 'w') as f:
                json.dump(data, f)
    
    def add_result(self, cr, ar, video_id):
        """Add a result and mark URL as processed (thread-safe)"""
        with self._lock:
            url_key = f"{cr}_{ar}"
            if url_key not in self.processed_urls:
                self.processed_urls.add(url_key)
                if video_id:
                    self.results.append({'cr': cr, 'ar': ar, 'video_id': video_id})
    
    def is_processed(self, cr, ar):
        """Check if URL was already processed (thread-safe)"""
        with self._lock:
            return f"{cr}_{ar}" in self.processed_urls

def process_single_url(args):
    """Process a single URL - designed for ThreadPoolExecutor"""
    cr, ar, thread_id, progress_tracker, browser_pool = args
    
    # Skip if already processed
    if progress_tracker.is_processed(cr, ar):
        print(f"Thread {thread_id}: SKIPPED - {cr} (already processed)")
        return None
    
    browser = None
    try:
        browser = browser_pool.get_browser()
        
        print(f"Thread {thread_id}: Processing {cr}")
        start_time = time.time()
        
        video_id = extract_video_id_with_selenium(browser, cr, ar)
        elapsed = time.time() - start_time
        
        # Add to progress tracker
        progress_tracker.add_result(cr, ar, video_id)
        
        if video_id:
            print(f"Thread {thread_id}: SUCCESS - {cr} -> {video_id} ({elapsed:.2f}s)")
            result = {'cr': cr, 'ar': ar, 'video_id': video_id}
        else:
            print(f"Thread {thread_id}: FAILED - {cr} ({elapsed:.2f}s)")
            result = None
        
        # Add random delay to mimic human behavior and avoid detection
        delay = random.uniform(5, 15)  # Random delay between 5-15 seconds
        time.sleep(delay)
        return result
        
    except Exception as e:
        error_msg = str(e)
        elapsed = time.time() - start_time if 'start_time' in locals() else 0
        
        # Check if it's a rate limiting error and handle specially
        if "RATE LIMITED" in error_msg:
            print(f"Thread {thread_id}: ⚠️  RATE LIMITED - {cr}: {error_msg} ({elapsed:.2f}s)")
            print(f"🚨 Backing off for 60 seconds due to rate limiting...")
            time.sleep(60)  # Wait 60 seconds on rate limit
        elif "SUSPICIOUS" in error_msg:
            print(f"Thread {thread_id}: ⚠️  SUSPICIOUS RESPONSE - {cr}: {error_msg} ({elapsed:.2f}s)")
        else:
            print(f"Thread {thread_id}: ERROR - {cr}: {error_msg} ({elapsed:.2f}s)")
        
        progress_tracker.add_result(cr, ar, None)
        return None
    finally:
        if browser:
            browser_pool.return_browser(browser)

def main():
    print("=== Video ID Scraping with Selenium ===")
    
    # Get input file name
    file_input_name = input("Enter the CSV file name (without extension): ")
    input_file = f'{file_input_name}.csv'
    
    # Get output file name
    file_output_name = input("Enter the output CSV file name (without extension): ")
    output_file = f'{file_output_name}.csv'

    # Progress tracking
    progress_file = f'{file_input_name}_progress.json'
    progress_tracker = ProgressTracker(progress_file)
    
    # Read URLs
    urls_to_process = []
    try:
        with open(input_file, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header row
            for row in reader:
                cr = row[0]
                ar = row[1]
                urls_to_process.append((cr, ar))
    except FileNotFoundError:
        print(f"Error: File not found - {input_file}")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return
    
    print(f"Total URLs to process: {len(urls_to_process)}")
    print(f"Already processed: {len(progress_tracker.processed_urls)}")
    print(f"Remaining: {len(urls_to_process) - len(progress_tracker.processed_urls)}")
    
    # Settings for concurrent processing (reduced for rate limiting)
    max_workers = 3       # Reduced from 10 to avoid 429 errors
    browser_pool_size = 3 # Match workers
    save_interval = 25    # Save progress every N URLs
    
    # Create browser pool with Firefox
    browser_type = 'firefox'
    
    # Configure proxies (add your proxy list here)
    proxy_list = [
        # Add your proxies here - examples:
        # "192.168.1.100:8080",
        # "192.168.1.101:8080", 
        # "username:password@proxy.example.com:3128",
        # "proxy1.example.com:8080",
        # "proxy2.example.com:8080"
    ]
    
    print(f"Creating browser pool with {browser_pool_size} {browser_type} browsers...")
    browser_pool = BrowserPool(browser_pool_size, browser_type)
    
    # Add proxies to the pool if provided
    if proxy_list:
        browser_pool.add_proxies(proxy_list)
    
    print(f"Using {max_workers} worker threads")
    print("Starting processing...")
    
    start_time = time.time()
    processed_count = len(progress_tracker.processed_urls)
    successful_results = []
    
    try:
        # Prepare arguments for thread pool
        thread_args = []
        for i, (cr, ar) in enumerate(urls_to_process):
            thread_id = i % max_workers
            args = (cr, ar, thread_id, progress_tracker, browser_pool)
            thread_args.append(args)
        
        # Process URLs using thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_args = {executor.submit(process_single_url, args): args for args in thread_args}
            
            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_args):
                try:
                    result = future.result()
                    if result:
                        successful_results.append(result)
                    
                    completed += 1
                    
                    # Progress update
                    if completed % 10 == 0:
                        current_processed = len(progress_tracker.processed_urls)
                        newly_processed = current_processed - processed_count
                        progress_percent = (completed / len(thread_args)) * 100
                        
                        elapsed_time = time.time() - start_time
                        rate = completed / elapsed_time if elapsed_time > 0 else 0
                        eta = (len(thread_args) - completed) / rate if rate > 0 else 0
                        
                        print(f"\nProgress: {completed}/{len(thread_args)} ({progress_percent:.1f}%)")
                        print(f"Rate: {rate:.2f} URLs/min, ETA: {eta/60:.1f} minutes")
                        print(f"Successful extractions: {len(successful_results)}")
                    
                    # Save progress periodically
                    if completed % save_interval == 0:
                        progress_tracker.save_progress()
                        print("Progress saved")
                
                except Exception as e:
                    print(f"Future error: {e}")
    
    except KeyboardInterrupt:
        print("\nInterrupted by user. Saving progress...")
    except Exception as e:
        print(f"Execution error: {e}")
    finally:
        # Clean up
        browser_pool.close_all()
    
    # Final save
    progress_tracker.save_progress()
    
    # Final statistics
    total_time = time.time() - start_time
    final_processed = len(progress_tracker.processed_urls)
    
    print(f"\n=== FINAL RESULTS ===")
    print(f"Total time: {total_time/3600:.2f} hours")
    print(f"URLs processed: {final_processed}")
    print(f"Videos found: {len(progress_tracker.results)}")
    print(f"Success rate: {len(progress_tracker.results)/final_processed*100:.1f}%" if final_processed > 0 else "No URLs processed")
    print(f"Average time per URL: {total_time/final_processed:.2f}s" if final_processed > 0 else "N/A")
    
    # Save results to CSV
    output_file = f'{file_output_name}_results.csv'
    
    try:
        with open(output_file, mode='w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Creative_ID', 'Advertiser_ID', 'Video_ID'])
            for result in progress_tracker.results:
                writer.writerow([result['cr'], result['ar'], result['video_id']])
        
        print(f"Results saved to: {output_file}")
    except Exception as e:
        print(f"Error saving results: {e}")

if __name__ == "__main__":
    main()