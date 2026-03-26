# Graph Business Intelligence System

[![Live Demo](https://img.shields.io/badge/Live-Demo-green)](https://graph-business-system.vercel.app)
[![Backend API](https://img.shields.io/badge/Backend-API-blue)](https://graph-backend.onrender.com/docs)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

A powerful business intelligence system that transforms fragmented business data into an interactive graph, allowing natural language queries powered by LLM (Groq).

## Live Demo

- **Frontend**: [https://graph-business-system.vercel.app](https://graph-business-system.vercel.app)
- **Backend API**: [https://graph-backend.onrender.com/docs](https://graph-backend.onrender.com/docs)

## Features

### Core Features
- **Graph Construction**: Unified graph of 21,606 nodes and 51,926 relationships
- **Interactive Visualization**: React Force Graph with node expansion and metadata inspection
- **Natural Language Queries**: LLM-powered translation to Cypher queries
- **Real-time Responses**: Streaming responses from Groq's Mixtral model
- **Guardrails**: Intelligent off-topic query rejection

### Business Intelligence
- Product billing analysis
- Order flow tracing (Order → Delivery → Billing → Payment)
- Broken flow detection (delivered but not billed)
- Customer spending analytics
- Revenue analysis

### Technical Highlights
- Graph database (Neo4j) for relationship traversal
- Redis caching for performance
- Rate limiting (30 req/min)
- Docker containerization
- CI/CD ready

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Node.js 18+
- Python 3.11+
- Groq API Key

### Local Development

1. **Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/graph-business-system
cd graph-business-system
```
2. **Set up environment variables**
```bash
cp .env.example .env
# Add GROQ_API_KEY to .env
```

3. **Start services with Docker**
```bash
docker-compose up -d
```

4. **Access the application**

- Frontend: http://localhost

- Backend API: http://localhost:8000/docs

- Neo4j Browser: http://localhost:7474

5. **Manual Setup**
Backend:
```bash
cd backend
pip install -r requirements.txt
python run.py
```
Frontend:
```bash
cd frontend
npm install
npm run dev
```

## Performance Optimizations
- Redis caching (1 hour TTL for queries)
- Graph subgraph caching (30 min TTL)
- Query result pagination
- Rate limiting (30 requests/minute)
- Response streaming for better UX