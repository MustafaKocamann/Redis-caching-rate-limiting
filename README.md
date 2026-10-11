# ⚡ Production Redis for AI & LLM Systems

<p align="center">
  <img src="https://raw.githubusercontent.com/redis/redis/unstable/assets/redis-logo.png" alt="Redis Logo" width="160" />
</p>

<p align="center">
  <strong>Production-Grade Caching, Semantic Similarity Search, Rate Limiting, and Memory Persistence for High-Throughput LLM & RAG Architectures</strong>
</p>

<p align="center">
  <a href="https://www.python.org/downloads/release/python-3130/"><img src="https://img.shields.io/badge/Python-3.13%2B-blue?logo=python&logoColor=white" alt="Python 3.13+" /></a>
  <a href="https://redis.io/"><img src="https://img.shields.io/badge/Redis_Stack-7.4%2B-DC382D?logo=redis&logoColor=white" alt="Redis Stack" /></a>
  <a href="https://www.langchain.com/"><img src="https://img.shields.io/badge/LangChain-v1.4%2B-1C3C3C?logo=langchain&logoColor=white" alt="LangChain" /></a>
  <a href="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2"><img src="https://img.shields.io/badge/Embeddings-all--MiniLM--L6--v2-FFA800?logo=huggingface&logoColor=white" alt="HuggingFace" /></a>
  <a href="https://groq.com/"><img src="https://img.shields.io/badge/Fast_Inference-Groq-F55036" alt="Groq" /></a>
  <a href="https://www.docker.com/"><img src="https://img.shields.io/badge/Container-Docker_Compose-2496ED?logo=docker&logoColor=white" alt="Docker Compose" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License" /></a>
</p>

---

## 📑 Table of Contents

- [Architectural Overview](#-architectural-overview)
- [Why Redis for AI Systems?](#-why-redis-for-ai-systems)
- [Repository Structure](#-repository-structure)
- [Core Modules & Implementation Deep Dive](#-core-modules--implementation-deep-dive)
  - [1. Infrastructure & Core Setup (`setup.ipynb`)](#1-infrastructure--core-setup-setupipynb)
  - [2. Exact-Match Prompt Caching (`exact_match.ipynb`)](#2-exact-match-prompt-caching-exact_matchipynb)
  - [3. Semantic / Vector Caching (`semantic_caching.ipynb`)](#3-semantic--vector-caching-semantic_cachingipynb)
  - [4. Production LLM Rate Limiting (`rate_limiting.ipynb`)](#4-production-llm-rate-limiting-rate_limitingipynb)
  - [5. AI Agent Memory & Profile Persistence (`agent_memory.ipynb`)](#5-ai-agent-memory--profile-persistence-agent_memoryipynb)
  - [6. Three-Tier RAG Caching Pipeline (`rag_caching.ipynb`)](#6-three-tier-rag-caching-pipeline-rag_cachingipynb)
- [Redis Keyspace & Data Structures Topology](#-redis-keyspace--data-structures-topology)
- [Performance & Latency Benchmarks](#-performance--latency-benchmarks)
- [Getting Started & Installation](#-getting-started--installation)
  - [Prerequisites](#prerequisites)
  - [Environment Configuration](#environment-configuration)
  - [Starting Redis Stack](#starting-redis-stack)
  - [Installing Dependencies](#installing-dependencies)
- [Observability with RedisInsight](#-observability-with-redisinsight)
- [Production Best Practices & Checklist](#-production-best-practices--checklist)
- [License](#-license)

---

## 🏛️ Architectural Overview

Modern Generative AI and Large Language Model applications suffer from three critical bottlenecks:
1. **Substantial Latency:** Standard inference round-trips take between `800ms` and `4000ms`.
2. **Exponential API Costs:** Redundant queries repeatedly consume tokens.
3. **Strict Rate Limits & Ephemeral State:** Downstream LLM provider quotas get exhausted, while conversational agents lose state across sessions.

This repository provides an enterprise-ready engineering foundation using **Redis Stack as an ultra-low latency, in-memory orchestration layer**:

```
                                  [ User Request / Query ]
                                             │
                                             ▼
                             ┌───────────────────────────────┐
                             │  Tier 0: Token Bucket /       │
                             │  Rate Limiter (Atomic Redis)  │
                             └──────────────┬────────────────┘
                                            │ Allowed
                                            ▼
                             ┌───────────────────────────────┐
                             │  Tier 1: Exact-Match Cache    │──── (HIT: ~2ms) ───► [ Return Cached ]
                             │  (SHA-256 Normalized String)  │
                             └──────────────┬────────────────┘
                                            │ MISS
                                            ▼
                             ┌───────────────────────────────┐
                             │  Tier 2: Semantic Cache       │──── (HIT: ~4ms) ───► [ Return Cached ]
                             │  (Local MiniLM Embeddings     │
                             │   + Cosine Similarity Scan)   │
                             └──────────────┬────────────────┘
                                            │ MISS
                                            ▼
                      ┌─────────────────────────────────────────────┐
                      │    Tier 3: Three-Layer RAG Pipeline         │
                      │                                             │
                      │  ┌───────────────────────────────────────┐  │
                      │  │ 3.1 Document & Query Embedding Cache  │  │
                      │  │     (Permanent Keys: embedding:*)     │  │
                      │  └───────────────────┬───────────────────┘  │
                      │                      ▼                      │
                      │  ┌───────────────────────────────────────┐  │
                      │  │ 3.2 Top-K Retrieval Result Cache      │  │
                      │  │     (TTL 30m: retrieval:*)            │  │
                      │  └───────────────────┬───────────────────┘  │
                      │                      ▼                      │
                      │  ┌───────────────────────────────────────┐  │
                      │  │ 3.3 LLM Answer Synthesis Cache        │  │
                      │  │     (TTL 60m: rag:answer:*)           │  │
                      │  └───────────────────┬───────────────────┘  │
                      └──────────────────────┼──────────────────────┘
                                             │
                                             ▼
                             ┌───────────────────────────────┐
                             │  Agent Memory & State Layer   │
                             │  • Short-term Sliding Window  │
                             │  • Long-term User Profiles    │
                             └───────────────────────────────┘
```

---

## 💡 Why Redis for AI Systems?

| Dimension | Without Redis | With Redis AI Architecture | Improvement |
| :--- | :--- | :--- | :--- |
| **Response Latency** | 1,500 ms – 3,500 ms | **1.2 ms – 4.5 ms** (on cache hit) | **~99.8% Faster** |
| **Inference Cost** | $0.0015 – $0.03 per call | **$0.0000** (Zero API cost on hit) | **100% Cost Elimination** |
| **Provider Rate Limiting** | Prone to `429 Too Many Requests` | Strict, atomic client-side sliding windows | **Zero 429 Cascading Errors** |
| **Agent History** | In-memory only (lost on restart) | Redis Lists with TTL & `LTRIM` sliding window | **Stateful, Resilient Sessions** |
| **Embedding Generation** | Re-computed every request | Hash-indexed, cached permanent embeddings | **Sub-millisecond retrieval** |

---

## 📂 Repository Structure

```plaintext
Redis-caching-rate-limiting/
├── .env.example             # Configuration blueprint for API keys and Redis host
├── .gitignore               # Ignored environments, checkpoints, and bytecode
├── docker-compose.yml       # Production-ready Redis Stack + RedisInsight deployment
├── pyproject.toml           # Modern Python packaging configuration (uv/pip)
├── uv.lock                  # Deterministic dependency lockfile
├── main.py                  # Entrypoint smoke-test application
│
├── setup.ipynb              # 01. Redis setup, connection health, and fundamental primitives
├── exact_match.ipynb        # 02. Exact-match prompt caching, hashing, token cost analytics
├── semantic_caching.ipynb   # 03. Vector similarity, SentenceTransformers, cosine thresholding
├── rate_limiting.ipynb      # 04. Fixed & sliding window rate limiters with LLM protection
├── agent_memory.ipynb       # 05. Stateful agent memory, sliding conversation windows, user profiles
└── rag_caching.ipynb        # 06. 3-Tier Enterprise RAG caching (Embeddings, Retrieval, Answers)
```

---

## 🔬 Core Modules & Implementation Deep Dive

### 1. Infrastructure & Core Setup (`setup.ipynb`)
- **Redis Stack Integration:** Bootstraps Redis 7.4+ along with the RedisInsight visualization engine.
- **Connection Diagnostics:** Validates client pings, server memory allocations (`used_memory_human`), and key distribution.
- **Fundamental Data Structures:**
  - `Strings`: Primitive value storage with atomic operations.
  - `TTL & Key Expiration`: Automated eviction policies (`SET key val EX seconds`).
  - `LLM Connectivity`: Establishes initial handshakes with OpenAI-compatible APIs (DeepSeek / Evren API) via LangChain `ChatOpenAI`.

---

### 2. Exact-Match Prompt Caching (`exact_match.ipynb`)
- **Key Normalization Engine:** Eliminates whitespace anomalies and case sensitivity variance:
  $$\text{Normalized} = \text{lowercase}(\text{strip}(P))$$
  $$\text{Key} = \text{"llm:cache:"} + \text{SHA256}(\text{Normalized})$$
- **Payload Schema:** Stores serialized JSON hashes with comprehensive telemetry:
  ```json
  {
    "prompt": "What is supervised machine learning?",
    "response": "Supervised learning is...",
    "model": "deepseek-v4-flash",
    "input_tokens": 14,
    "output_tokens": 85,
    "cached_at": 1773312000.0
  }
  ```
- **Cost & Latency Tracking:** Calculates precise cost savings per cache hit based on dynamic token pricing models ($0.15/M input tokens, $0.60/M output tokens).

---

### 3. Semantic / Vector Caching (`semantic_caching.ipynb`)
Handles queries that are semantically identical but syntactically distinct (e.g., *"What is machine learning?"* vs. *"Can you explain ML?"*).

```
"What is machine learning?"  ──► [ all-MiniLM-L6-v2 ] ──► [0.052, -0.012, ..., 0.089]
                                                                     │
                                                             Cosine Similarity
                                                              Score: 0.912 ≥ 0.60
                                                                     │
"Define machine learning"    ──► [ all-MiniLM-L6-v2 ] ──► [0.048, -0.010, ..., 0.092]
                                                                     ▼
                                                             [ INSTANT CACHE HIT ]
```

- **Embedding Model:** Local, lightweight HuggingFace model (`all-MiniLM-L6-v2`) outputting 384-dimensional dense vectors with zero API overhead.
- **Similarity Metric:** High-performance vectorized Cosine Similarity:
  $$\text{Cosine Similarity}(\vec{u}, \vec{v}) = \frac{\vec{u} \cdot \vec{v}}{\|\vec{u}\|_2 \|\vec{v}\|_2}$$
- **Threshold Sensitivity Analysis:**
  - $\ge 0.85$: Very strict (nearly identical syntax and intent).
  - $\ge 0.70$: High semantic correlation (safe for production enterprise paraphrases).
  - $\ge 0.60$: Balanced recall (captures varied phrasing while maintaining precision).
  - $< 0.50$: Uncorrelated prompts (routed to LLM).

---

### 4. Production LLM Rate Limiting (`rate_limiting.ipynb`)
Protects external LLM endpoints from budget runaway, burst traffic, and provider API throttling (HTTP 429).

#### A. Fixed Window Limiter
- Uses atomic Redis `INCR` operations with automatic window expiry.
- Undoes increments on blocked requests to prevent quota penalization.

#### B. Precision Sliding Window Limiter
- Implemented using **Redis Sorted Sets (`ZSET`)** with sub-second timestamps as scores.
- Atomic execution inside a Redis Pipeline:
  1. `ZREMRANGEBYSCORE(key, 0, now - window)` &rarr; Drops expired requests.
  2. `ZCARD(key)` &rarr; Counts requests within the sliding window.
  3. `ZADD(key, {now: now})` &rarr; Records the current request.
  4. `EXPIRE(key, window + 1)` &rarr; Ensures garbage collection.

#### C. LLM Guard Contract
Provides clean structured responses when rate limits are exceeded:
```json
{
  "success": false,
  "error": "rate_limited",
  "message": "Rate limit reached. Please wait 42 seconds.",
  "reset_in_seconds": 42
}
```

---

### 5. AI Agent Memory & Profile Persistence (`agent_memory.ipynb`)
Solves the stateless nature of conversational agents by providing distinct memory layers.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        AGENT MEMORY SUBSYSTEM                          │
├───────────────────────────────────┬────────────────────────────────────┤
│   Short-Term Conversational Memory │    Long-Term User Profile Store   │
│   (Redis List: conversation:<id>) │    (Redis Hash: profile:<user_id>) │
├───────────────────────────────────┼────────────────────────────────────┤
│ • RPUSH / LTRIM sliding window    │ • HSET / HGETALL key-value facts   │
│ • Fixed token budget (e.g., 20 msg)│ • Persistent user role & context   │
│ • Auto-refreshing session TTL     │ • Injected into Agent system prompt│
│ • User-session isolation tests    │ • Never expires automatically      │
└───────────────────────────────────┴────────────────────────────────────┘
```

- **Sliding Window History:** Keeps only the last $N$ turns via `LTRIM key -N -1`, preventing context window overflow.
- **Multi-Tenant Isolation:** Guarantees strict boundary separation between independent user sessions.
- **Profile Injection:** Persistent user facts (role, preferred language, experience) are retrieved and injected into the LLM system prompt dynamically.

---

### 6. Three-Tier RAG Caching Pipeline (`rag_caching.ipynb`)
Enterprise Retrieval-Augmented Generation (RAG) caching architecture over a 15-document knowledge base.

```plaintext
Incoming Question
      │
      ▼
[ Tier 3: Final Answer Cache ] ──► (HIT: ~2 ms) ──► Return Synthesized Answer
      │ (MISS)
      ▼
[ Tier 1: Query Embedding Cache ] ──► Retrieve or Compute 384-dim Vector
      │
      ▼
[ Tier 2: Retrieval Result Cache ] ──► (HIT: ~1 ms) ──► Instant Top-K Documents
      │ (MISS)
      ▼
Vector Cosine Matcher (Knowledge Base)
      │
      ▼
Groq LLM Generation (openai/gpt-oss-120b)
      │
      ▼
Store Final Answer (TTL: 1 Hour)
```

- **Tier 1 (Embedding Cache):** Permanent key-value pairs (`embedding:minilm:<hash>`). Computes each document and query embedding once.
- **Tier 2 (Retrieval Cache):** 30-minute TTL (`retrieval:<hash>`). Caches ranked top-k document indices per unique query.
- **Tier 3 (Answer Cache):** 60-minute TTL (`rag:answer:<hash>`). Caches synthesized answers directly. Subsequent identical inquiries bypass both retrieval and LLM inference.

---

## 🗄️ Redis Keyspace & Data Structures Topology

| Component | Key Pattern | Redis Type | TTL Policy | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Exact Match** | `llm:cache:<hash>` | `String` (JSON) | 1 hour (`3600s`) | Cached LLM response, token stats & metadata |
| **Semantic Entry** | `sem:cache:<entry_id>` | `Hash` | Indefinite / Config | Prompt, response, embedding vector & timestamp |
| **Semantic Index** | `sem:cache:index` | `Set` | Permanent | Set of all active semantic cache entry IDs |
| **Fixed Rate Limit** | `rate:fixed:<identifier>` | `String` (Integer) | Window duration | Atomic counter for fixed-window quotas |
| **Sliding Rate Limit** | `rate:sliding:<identifier>` | `Sorted Set` | Window duration + 1s | Timestamp-scored sorted set for sliding window |
| **Agent Chat History**| `conversation:<session_id>`| `List` | 2 hours (`7200s`) | Rolling conversational message history |
| **User Profile** | `profile:<user_id>` | `Hash` | Permanent | Long-term user metadata and preferences |
| **RAG Embeddings** | `embedding:minilm:<hash>` | `String` (JSON) | Permanent | 384-dim normalized vector embeddings |
| **RAG Retrieval** | `retrieval:<hash>` | `String` (JSON) | 30 minutes (`1800s`)| Top-K retrieved documents and scores |
| **RAG Answer** | `rag:answer:<hash>` | `String` (JSON) | 1 hour (`3600s`) | Final RAG synthesized response |

---

## 📊 Performance & Latency Benchmarks

Empirical benchmarks recorded across execution passes:

```
[RAG First Pass: Full Pipeline Computation]
════════════════════════════════════════════════════════════════
  Status       : COMPUTED (Embedding + Retrieval + Groq LLM)
  Latency      : 2244.18 ms
  API Cost     : Standard Token Rates
════════════════════════════════════════════════════════════════

[RAG Second Pass: Tier-3 Answer Cache Hit]
════════════════════════════════════════════════════════════════
  Status       : ⚡ ANSWER CACHE HIT (Pure Redis Retrieval)
  Latency      : 1.10 ms  (🚀 99.95% Speedup)
  API Cost     : $0.00000000 (100% Savings)
════════════════════════════════════════════════════════════════
```

### Multi-Query Cache Hit-Rate Test (7 Questions)
- **Computed Queries:** 2 (Initial unseen queries)
- **Cache Hits:** 5 (Repeated / redundant queries)
- **Hit Rate:** **71%**
- **Average Cached Latency:** **2.14 ms** vs. **593.0 ms** computed.

---

## 🚀 Getting Started & Installation

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Engine version 20.10+)
- [Python 3.13+](https://www.python.org/)
- [`uv`](https://github.com/astral-sh/uv) (Recommended for lightning-fast environment setup) or `pip`

### Environment Configuration

Create a `.env` file in the project root:

```bash
# Redis Server Connection
REDIS_HOST=localhost
REDIS_PORT=6379

# LLM Provider Configuration (OpenAI Compatible / Evren API)
EVREN_API_KEY=your_llm_api_key_here
EVREN_BASE_URL=https://evren-llmapi.ssyz.org.tr/v1
EVREN_MODEL=deepseek-v4-flash

# Groq Cloud API (For ultra-fast RAG inference)
GROQ_API_KEY=your_groq_api_key_here
```

### Starting Redis Stack

Launch Redis Stack and RedisInsight via Docker Compose:

```bash
docker-compose up -d
```

Verify that the container is healthy:
```bash
docker ps --filter "name=redis-ai-course"
```

### Installing Dependencies

Using `uv`:
```bash
# Install dependencies from pyproject.toml
uv sync
```

Alternatively, using standard `pip`:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
```

---

## 🖥️ Observability with RedisInsight

Redis Stack comes bundled with **RedisInsight**, providing a visual management dashboard for inspecting keys, vector values, and TTLs:

1. Open your browser and navigate to:
   ```
   http://localhost:8001
   ```
2. Connect to the local Redis instance (`host: redis-stack`, `port: 6379`).
3. Real-time visualization features:
   - **Key Tree Browser:** Inspect `embedding:*`, `rag:answer:*`, and `conversation:*`.
   - **TTL Countdown:** Watch keys expire in real-time according to configured policies.
   - **Memory Profiler:** Inspect RAM consumption across different data types.

---

## 🛡️ Production Best Practices & Checklist

- [x] **Key Namespacing:** Always prefix keys with domain identifiers (`rag:answer:`, `embedding:minilm:`).
- [x] **Atomic Rate Limiting:** Avoid race conditions by using Redis Pipelines and atomic `INCR` / `ZSET` commands.
- [x] **Safe Serialization:** Use `json.dumps()` and `.tolist()` on vector tensors for cross-language compatibility.
- [x] **Eviction Policy Tuning:** In memory-constrained production environments, configure `maxmemory-policy volatile-lru` or `allkeys-lru`.
- [x] **Connection Pooling:** Reuse Redis client instances across application workers rather than recreating connections per request.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE). Feel free to adapt and integrate these caching patterns into your commercial and open-source AI projects.
