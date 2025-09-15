import os
import sys
import json
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import re
import time

STOPWORDS = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
             'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
             'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
             'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
             'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it',
             'we', 'they', 'what', 'which', 'who', 'when', 'where', 'why', 'how',
             'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other', 'some',
             'such', 'as', 'also', 'very', 'too', 'only', 'so', 'than', 'not'}



def query(search_query, max_results, log_file, start=0, max_retries=3):
    base_url = "http://export.arxiv.org/api/query"
    
    params = {
        'search_query': search_query,
        'start': start,
        'max_results': max_results
    }
    
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    
    # Log the query
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] Starting ArXiv query: {search_query}\n")
    
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                if response.status == 429:
                    if attempt < max_retries - 1:
                        # Wait 3 seconds bf continue
                        time.sleep(3)
                        continue
                    else:
                        raise Exception("Rate limit exceeded after maximum retries")
                
                xml_content = response.read().decode('utf-8')
                
                timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"[{timestamp}] Fetched {len(xml_content)} bytes from ArXiv API\n")
                
                return xml_content
                
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries - 1:
                time.sleep(3)
                continue
            raise Exception(f"HTTP Error {e.code}: {e.reason}")
        except Exception as e:
            # Network errors
            if attempt == max_retries - 1:
                raise Exception(f"Network error: {str(e)}")
            time.sleep(1)
    
    raise Exception("Failed to query ArXiv API after all retries")


def parse_xml_and_analyze(xml_content, log_file):
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        # Invalid XML
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] Invalid XML: {str(e)}\n")
        return []

    namespaces = {
        'atom': 'http://www.w3.org/2005/Atom',
        'arxiv': 'http://arxiv.org/schemas/atom'
    }
    
    papers = []
    
    for entry in root.findall('atom:entry', namespaces):
        try:
            # ID extraction
            id_elem = entry.find('atom:id', namespaces)
            if id_elem is not None:
                paper_id = id_elem.text.split('/')[-1]
                timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"[{timestamp}] Processing paper: {paper_id}\n")
            else:
                # Missing fields handling
                timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"[{timestamp}] Missing paper ID, skipping paper\n")
                continue
            
            # Title extraction
            title_elem = entry.find('atom:title', namespaces)
            if title_elem is not None:
                title = title_elem.text.strip().replace('\n', ' ')
            else:
                timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"[{timestamp}] Missing title for paper {paper_id}\n")
                title = ""
            
            # Authors extraction
            authors = []
            for author in entry.findall('atom:author', namespaces):
                name_elem = author.find('atom:name', namespaces)
                if name_elem is not None:
                    authors.append(name_elem.text.strip())
            
            # Abstract extraction
            summary_elem = entry.find('atom:summary', namespaces)
            if summary_elem is not None:
                abstract = summary_elem.text.strip().replace('\n', ' ')
            else:
                timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"[{timestamp}] Missing abstract for paper {paper_id}\n")
                abstract = ""
            
            # Categories extraction
            categories = []
            for category in entry.findall('atom:category', namespaces):
                term = category.get('term')
                if term:
                    categories.append(term)
            
            # Published date extraction
            published_elem = entry.find('atom:published', namespaces)
            if published_elem is not None:
                published = published_elem.text.strip()
            else:
                published = ""
            
            # Updated date extraction
            updated_elem = entry.find('atom:updated', namespaces)
            if updated_elem is not None:
                updated = updated_elem.text.strip()
            else:
                updated = ""

            abstract_stats = {"total_words": 0, "unique_words": 0, "total_sentences": 0, 
                            "avg_words_per_sentence": 0, "avg_word_length": 0}
            
            if abstract:
                words = re.findall(r'\b[a-zA-Z]+\b', abstract.lower())
                word_lengths = [len(word) for word in words]
                unique_words = set(words)
                
                sentences = re.split(r'[.!?]+', abstract)
                sentences = [s.strip() for s in sentences if s.strip()]
                
                sentence_word_counts = []
                for sentence in sentences:
                    sentence_words = re.findall(r'\b[a-zA-Z]+\b', sentence)
                    sentence_word_counts.append(len(sentence_words))
                
                abstract_stats = {
                    "total_words": len(words),
                    "unique_words": len(unique_words),
                    "total_sentences": len(sentences),
                    "avg_words_per_sentence": sum(sentence_word_counts) / len(sentence_word_counts) if sentence_word_counts else 0,
                    "avg_word_length": sum(word_lengths) / len(word_lengths) if word_lengths else 0
                }
            
            results = {
                "arxiv_id": paper_id,
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "categories": categories,
                "published": published,
                "updated": updated,
                "abstract_stats": abstract_stats
            }
            
            papers.append(results)
            
        except Exception as e:
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] Error processing paper: {str(e)}\n")
            continue
    
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] Fetched {len(papers)} results from ArXiv API\n")
    
    return papers


def generate_corpus_analysis(papers, query):
    if not papers:
        return {
            "query": query,
            "papers_processed": 0,
            "processing_timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "corpus_stats": {},
            "top_50_words": [],
            "technical_terms": {},
            "category_distribution": {}
        }
    
    all_abstracts = [paper.get('abstract', '') for paper in papers]
    all_text = ' '.join(all_abstracts)
    
    words = re.findall(r'\b[a-zA-Z]+\b', all_text.lower())
    filtered_words = [word for word in words if word not in STOPWORDS and len(word) > 2]
    
    word_freq = {}
    for word in filtered_words:
        word_freq[word] = word_freq.get(word, 0) + 1
    
    top_50_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:50]
    top_50_formatted = []
    for word, freq in top_50_words:
        doc_count = sum(1 for abstract in all_abstracts if word in abstract.lower())
        top_50_formatted.append({
            "word": word,
            "frequency": freq,
            "documents": doc_count
        })
    
    all_uppercase = []
    all_numeric = []
    all_hyphenated = []
    
    for paper in papers:
        abstract = paper.get('abstract', '')
        if abstract:
            uppercase_terms = re.findall(r'\b[A-Z]{2,}\b', abstract)
            numeric_terms = re.findall(r'\b\w*\d+\w*\b', abstract)
            hyphenated_terms = re.findall(r'\b\w+-\w+(?:-\w+)*\b', abstract)
            
            all_uppercase.extend(uppercase_terms)
            all_numeric.extend(numeric_terms)
            all_hyphenated.extend(hyphenated_terms)
    
    category_dist = {}
    for paper in papers:
        for category in paper.get('categories', []):
            category_dist[category] = category_dist.get(category, 0) + 1
    
    total_abstracts = len([p for p in papers if p.get('abstract')])
    total_words = sum(paper.get('abstract_stats', {}).get('total_words', 0) for paper in papers)
    unique_words_global = len(set(filtered_words))
    avg_abstract_length = total_words / total_abstracts if total_abstracts > 0 else 0
    
    abstract_lengths = [(paper.get('abstract_stats', {}).get('total_words', 0), paper.get('arxiv_id', ''))
                       for paper in papers if paper.get('abstract')]
    longest_abstract = max(abstract_lengths, key=lambda x: x[0]) if abstract_lengths else (0, '')
    shortest_abstract = min(abstract_lengths, key=lambda x: x[0]) if abstract_lengths else (0, '')
    
    return {
        "query": query,
        "papers_processed": len(papers),
        "processing_timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "corpus_stats": {
            "total_abstracts": total_abstracts,
            "total_words": total_words,
            "unique_words_global": unique_words_global,
            "avg_abstract_length": avg_abstract_length,
            "longest_abstract_words": longest_abstract[0],
            "shortest_abstract_words": shortest_abstract[0]
        },
        "top_50_words": top_50_formatted,
        "technical_terms": {
            "uppercase_terms": list(set(all_uppercase)),
            "numeric_terms": list(set(all_numeric)),
            "hyphenated_terms": list(set(all_hyphenated))
        },
        "category_distribution": category_dist
    }



def main():
    if len(sys.argv) != 4:
        print("Usage: arxiv_processor.py <search_query> <max_results> <output_directory>")
        print("Example: arxiv_processor.py 'cat:cs.LG' 10 output/")
        sys.exit(1)

    start_time = time.time()
    
    search_query = sys.argv[1]
    try:
        max_results = int(sys.argv[2])
        if max_results < 1 or max_results > 100:
            raise ValueError("max_results must be between 1 and 100")
    except ValueError as e:
        print(f"Error: max_results must be a positive integer between 1 and 100")
        sys.exit(1)
    
    output_dir = sys.argv[3]
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    log_file = os.path.join(output_dir, 'processing.log')
    with open(log_file, 'w') as f:
        pass
    
    try:
        # Query API
        xml_response = query(search_query, max_results, log_file)
        
        # Parse the XML response and analysis
        papers = parse_xml_and_analyze(xml_response, log_file)
        
        print(f"Finished fetching {len(papers)} papers ...")
        
        # Calculate metrics
        print(f"Calculating metrics ...")
        corpus_analysis = generate_corpus_analysis(papers, search_query)

        print(f"Writing results to {output_dir} ...")
        
        # Svae papers.json
        papers_file = os.path.join(output_dir, 'papers.json')
        with open(papers_file, 'w', encoding='utf-8') as f:
            json.dump(papers, f, indent=2, ensure_ascii=False)
        
        # Save corpus_analysis.json
        corpus_file = os.path.join(output_dir, 'corpus_analysis.json')
        with open(corpus_file, 'w', encoding='utf-8') as f:
            json.dump(corpus_analysis, f, indent=2, ensure_ascii=False)
        

        
        end_time = time.time()
        processing_time = round(end_time - start_time, 2)
        
        # Part C: Output Files - Final log entry
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] Completed processing: {len(papers)} papers in {processing_time} seconds\n")
        
        print(f"Successfully saved files to {output_dir} ...")
        
    except Exception as e:
        # Part D: Error Handling - Network errors, log and exit with code 1
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] Fatal error: {str(e)}\n")
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()