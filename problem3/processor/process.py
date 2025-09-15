import os
import sys
import json
import time
import re
from datetime import datetime, timezone


def strip_html(html_content):
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL)
    
    links = re.findall(r'href=["\']?([^"\' >]+)', html_content, flags=re.IGNORECASE)
    
    images = re.findall(r'src=["\']?([^"\' >]+)', html_content, flags=re.IGNORECASE)
    
    text = re.sub(r'<[^>]+>', ' ', html_content)
    
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text, links, images


def analyze_text(text):
    if not text:
        return {
            "word_count": 0,
            "sentence_count": 0,
            "paragraph_count": 0,
            "avg_word_length": 0
        }
    
    words = re.findall(r'\b[a-zA-Z]+\b', text)
    word_count = len(words)
    
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    sentence_count = len(sentences)
    
    paragraphs = re.split(r'\n\s*\n|\s{3,}', text)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    paragraph_count = len(paragraphs)
    
    word_lengths = [len(word) for word in words]
    avg_word_length = sum(word_lengths) / len(word_lengths) if word_lengths else 0
    
    return {
        "word_count": word_count,
        "sentence_count": sentence_count,
        "paragraph_count": paragraph_count,
        "avg_word_length": avg_word_length
    }


def wait_for_fetch_complete():
    fetch_complete_path = "/shared/status/fetch_complete.json"
    
    while not os.path.exists(fetch_complete_path):
        time.sleep(1)


def process_html_files():
    raw_dir = "/shared/raw"
    processed_dir = "/shared/processed"
    
    os.makedirs(processed_dir, exist_ok=True)
    
    if not os.path.exists(raw_dir):
        return 0
    
    html_files = [f for f in os.listdir(raw_dir) if f.endswith('.html')]
    
    if not html_files:
        return 0
    
    processed_count = 0
    
    for html_file in html_files:
        try:
            html_path = os.path.join(raw_dir, html_file)
            with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
                html_content = f.read()
            
            text, links, images = strip_html(html_content)
            
            statistics = analyze_text(text)
            
            words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
            
            output_data = {
                "source_file": html_file,
                "text": text,
                "words": words,
                "statistics": statistics,
                "links": links,
                "images": images,
                "processed_at": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            
            output_filename = f"page_{processed_count + 1}.json"
            output_path = os.path.join(processed_dir, output_filename)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            processed_count += 1
            
        except Exception:
            continue
    
    return processed_count


def create_process_complete_status(processed_count):
    status_dir = "/shared/status"
    os.makedirs(status_dir, exist_ok=True)
    
    status_data = {
        "status": "complete",
        "processed_files": processed_count,
        "completed_at": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    
    status_path = os.path.join(status_dir, "process_complete.json")
    with open(status_path, 'w', encoding='utf-8') as f:
        json.dump(status_data, f, indent=2)
    



def main():
    print("HTML Processor starting...")
    
    try:
        wait_for_fetch_complete()
        
        processed_count = process_html_files()
        
        print(f"Finished processing {processed_count} HTML files")
        
        create_process_complete_status(processed_count)
        
        print("Processing complete!")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
