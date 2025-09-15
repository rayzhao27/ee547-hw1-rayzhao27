#!/usr/bin/env python3

import os
import json
import time
from datetime import datetime, timezone
from collections import Counter
import re

def jaccard_similarity(doc1_words, doc2_words):
    set1 = set(doc1_words)
    set2 = set(doc2_words)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0.0

def extract_ngrams(words, n):
    if len(words) < n:
        return []
    return [' '.join(words[i:i+n]) for i in range(len(words) - n + 1)]

def calculate_readability_metrics(text):
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    
    words = re.findall(r'\b\w+\b', text.lower())
    
    if not sentences or not words:
        return {
            "avg_sentence_length": 0.0,
            "avg_word_length": 0.0,
            "complexity_score": 0.0
        }
    
    avg_sentence_length = len(words) / len(sentences)
    avg_word_length = sum(len(word) for word in words) / len(words)
    
    complexity_score = (avg_sentence_length * 0.1) + (avg_word_length * 0.5)
    
    return {
        "avg_sentence_length": round(avg_sentence_length, 2),
        "avg_word_length": round(avg_word_length, 2),
        "complexity_score": round(complexity_score, 2)
    }

def wait_for_processing_complete():
    status_file = "/shared/status/process_complete.json"
    
    print("Waiting for processing to complete...")
    while not os.path.exists(status_file):
        time.sleep(5)
        print("Still waiting for process_complete.json...")
    
    print("Processing complete! Starting analysis...")

def load_processed_documents():
    processed_dir = "/shared/processed"
    documents = {}
    
    if not os.path.exists(processed_dir):
        raise FileNotFoundError(f"Processed directory not found: {processed_dir}")
    
    for filename in os.listdir(processed_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(processed_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    doc_data = json.load(f)
                    documents[filename] = doc_data
            except Exception as e:
                print(f"Error loading {filename}: {e}")
    
    return documents

def compute_global_statistics(documents):
    all_words = []
    all_texts = []
    doc_word_lists = {}
    
    for doc_name, doc_data in documents.items():
        words = doc_data.get('words', [])
        text = doc_data.get('text', '')
        
        all_words.extend(words)
        all_texts.append(text)
        doc_word_lists[doc_name] = words
    
    word_freq = Counter(all_words)
    top_100_words = [
        {
            "word": word,
            "count": count,
            "frequency": round(count / len(all_words), 6)
        }
        for word, count in word_freq.most_common(100)
    ]
    
    doc_names = list(documents.keys())
    similarity_matrix = []
    
    for i, doc1 in enumerate(doc_names):
        for j, doc2 in enumerate(doc_names):
            if i < j:
                similarity = jaccard_similarity(
                    doc_word_lists[doc1],
                    doc_word_lists[doc2]
                )
                similarity_matrix.append({
                    "doc1": doc1,
                    "doc2": doc2,
                    "similarity": round(similarity, 3)
                })
    
    bigrams = []
    trigrams = []
    
    for words in doc_word_lists.values():
        bigrams.extend(extract_ngrams(words, 2))
        trigrams.extend(extract_ngrams(words, 3))
    
    bigram_freq = Counter(bigrams)
    trigram_freq = Counter(trigrams)
    
    top_bigrams = [
        {"bigram": bigram, "count": count}
        for bigram, count in bigram_freq.most_common(50)
    ]
    
    top_trigrams = [
        {"trigram": trigram, "count": count}
        for trigram, count in trigram_freq.most_common(50)
    ]
    
    readability_scores = []
    for doc_data in documents.values():
        text = doc_data.get('text', '')
        if text:
            readability_scores.append(calculate_readability_metrics(text))
    
    if readability_scores:
        avg_readability = {
            "avg_sentence_length": round(
                sum(r["avg_sentence_length"] for r in readability_scores) / len(readability_scores), 2
            ),
            "avg_word_length": round(
                sum(r["avg_word_length"] for r in readability_scores) / len(readability_scores), 2
            ),
            "complexity_score": round(
                sum(r["complexity_score"] for r in readability_scores) / len(readability_scores), 2
            )
        }
    else:
        avg_readability = {
            "avg_sentence_length": 0.0,
            "avg_word_length": 0.0,
            "complexity_score": 0.0
        }
    
    return {
        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
        "documents_processed": len(documents),
        "total_words": len(all_words),
        "unique_words": len(word_freq),
        "top_100_words": top_100_words,
        "document_similarity": similarity_matrix,
        "top_bigrams": top_bigrams,
        "top_trigrams": top_trigrams,
        "readability": avg_readability
    }

def save_final_report(analysis_results):
    analysis_dir = "/shared/analysis"
    os.makedirs(analysis_dir, exist_ok=True)
    
    report_path = os.path.join(analysis_dir, "final_report.json")
    
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(analysis_results, f, indent=2, ensure_ascii=False)
    
    print(f"Final report saved to: {report_path}")

def main():
    try:
        wait_for_processing_complete()
        
        print("Loading processed documents...")
        documents = load_processed_documents()
        print(f"Loaded {len(documents)} documents")
        
        print("Computing global statistics...")
        analysis_results = compute_global_statistics(documents)
        
        print("Saving final report...")
        save_final_report(analysis_results)
        
        print("Analysis complete!")
        
        print(f"\nSummary:")
        print(f"- Documents processed: {analysis_results['documents_processed']}")
        print(f"- Total words: {analysis_results['total_words']}")
        print(f"- Unique words: {analysis_results['unique_words']}")
        print(f"- Top word: '{analysis_results['top_100_words'][0]['word']}' ({analysis_results['top_100_words'][0]['count']} occurrences)")
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    main()
