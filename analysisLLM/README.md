# analysisLLM

This module performs LLM-based analysis on scraped Douban posts.

Current features:
- Fan / hater / neutral classification
- Sentiment and sarcasm detection
- JSON output stored in PostgreSQL

Model:
- qwen3:4b via Ollama

Entry point:
- label_posts_with_llm.py
