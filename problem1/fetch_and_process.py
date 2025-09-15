import os
import sys
import time
import json
import urllib.request

from datetime import datetime, timezone


def fetch(url):
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        start_time = time.time()

        # 1. Perform an HTTP GET request to the URL
        response = urllib.request.urlopen(url, timeout=10)
        end_time = time.time()

        # 2. Measure the response time in milliseconds
        fetch_time = float((end_time - start_time) * 1000)

        # 3. Capture the HTTP status code
        status_code = response.status

        # 4. Calculate the size of the response body in bytes
        content = response.read()
        content_length = len(content)

        # 5.Count the number of words in the response (for text responses only)
        if 'text' in response.headers.get('content-type', '').lower():
            text = content.decode('utf-8', errors='ignore')
            word_count = len(text.split())
        else:
            word_count = None

        # Construct results (responses.json)
        result = {
            "url": url,
            "status_code": status_code,
            "response_time_ms": fetch_time,
            "content_length": content_length,
            "word_count": word_count,
            "timestamp": timestamp,
            "error": "null"
        }

        return result

    except Exception as e:
        result = {
            "url": url,
            "status_code": None,
            "response_time_ms": None,
            "content_length": None,
            "word_count": None,
            "timestamp": timestamp,
            "error": str(e)
        }

        return result

def main():
    # Some pre-checking
    if len(sys.argv) != 3:
        print("Usage: fetch_and_process.py <url> <output directory>")
        sys.exit(1)

    processing_start_timestamp = datetime.now(timezone.utc).isoformat()

    url_file = sys.argv[1]
    output_dir = sys.argv[2]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Open input file:
    with open(url_file, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]

    # Fetching urls
    results = []
    for url in urls:
        print(f"Fetching: {url} ...")
        result = fetch(url)
        results.append(result)

    print(f"Finished fetching {len(urls)} urls ...")

    # Calculating metrics
    print(f"Calculating metrics ...")
    total_num = len(results)
    successful = [r for r in results if r["error"]=="null"]
    failed_num = total_num - len(successful)

    response_times_ms = [r['response_time_ms'] for r in successful if r['response_time_ms']]
    total_bytes = [r['content_length'] for r in successful if r['content_length']]

    status_code_distribution = {}
    for result in successful:
        code = str(result['status_code'])
        if code in status_code_distribution:
            status_code_distribution[code] += 1
        else:
            status_code_distribution[code] = 1

    processing_end_timestamp = datetime.now(timezone.utc).isoformat()

    summary = {
        "total_urls": total_num,
        "successful_requests": len(successful),
        "failed_requests": failed_num,
        "avg_response_time_ms": sum(response_times_ms) / len(response_times_ms) if response_times_ms else 0,
        "total_bytes_downloaded": sum(total_bytes) if total_bytes else 0,
        "status_code_distribution": status_code_distribution,
        "processing_start": processing_start_timestamp,
        "processing_end": processing_end_timestamp
    }

    # Saving files
    print(f"Writing results to {output_dir} ...")

    with open(os.path.join(output_dir, 'responses.json'), 'w') as f:
        json.dump(results, f, indent=2)

    with open(os.path.join(output_dir, 'summary.json'), 'w') as f:
        json.dump(summary, f, indent=2)

    with open(os.path.join(output_dir, 'errors.log'), 'w') as f:
        failed_results = [r for r in results if r['error']!="null"]
        for result in failed_results:
            timestamp = result['timestamp']
            f.write(f"[{timestamp}] [{result['url']}]: {result['error']}\n")

    print(f"Successfully saved files to {output_dir} ...")


if __name__ == "__main__":
    main()
