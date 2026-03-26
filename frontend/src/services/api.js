import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 60000, // 60 second timeout for streaming
});

// Response interceptor
// Add request interceptor to handle rate limiting
api.interceptors.response.use(
  response => response,
  error => {
    if (error.response?.status === 429) {
      console.warn('Rate limit hit, waiting...');
      // You could add retry logic here
    }
    return Promise.reject(error);
  }
);

// Graph endpoints
export const getNodes = async (nodeType = null, limit = 50, skip = 0) => {
  const params = { limit, skip };
  if (nodeType) params.node_type = nodeType;
  const response = await api.get('/api/graph/nodes', { params });
  return response.data;
};

export const getNodeMetadata = async (nodeId) => {
  const response = await api.get(`/api/graph/node/${encodeURIComponent(nodeId)}`);
  return response.data;
};

export const getNeighbors = async (nodeId, depth = 1, limit = 50) => {
  const params = { depth, limit };
  const response = await api.get(`/api/graph/neighbors/${encodeURIComponent(nodeId)}`, { params });
  return response.data;
};

export const getSubgraph = async (nodeIds) => {
  const response = await api.post('/api/graph/subgraph', nodeIds);
  return response.data;
};

export const getSchema = async () => {
  const response = await api.get('/api/graph/schema');
  return response.data;
};

export const searchNodes = async (query, nodeType = null, limit = 20) => {
  const params = { query, limit };
  if (nodeType) params.node_type = nodeType;
  const response = await api.get('/api/graph/search', { params });
  return response.data;
};

export const getOverview = async () => {
  const response = await api.get('/api/graph/overview');
  return response.data;
};

// Chat endpoints with streaming
export const sendQuery = async (question, sessionId = null) => {
  const response = await api.post('/api/chat/query', { question, session_id: sessionId });
  return response.data;
};

export const sendQueryStream = async (question, sessionId = null, onMessage) => {
  try {
    const response = await fetch(`${API_BASE_URL}/api/chat/query-stream`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ question, session_id: sessionId }),
    });
    
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      
      const chunk = decoder.decode(value);
      const lines = chunk.split('\n');
      
      for (const line of lines) {
        if (line.trim()) {
          try {
            const data = JSON.parse(line);
            onMessage(data);
          } catch (e) {
            console.error('Failed to parse SSE data:', e);
          }
        }
      }
    }
  } catch (error) {
    console.error('Streaming error:', error);
    onMessage({ type: 'error', content: error.message });
  }
};

export const getSuggestions = async () => {
  const response = await api.get('/api/chat/suggestions');
  return response.data;
};

export const getStatistics = async () => {
  const response = await api.get('/api/graph/statistics');
  return response.data;
};

export const healthCheck = async () => {
  try {
    const response = await api.get('/health', { timeout: 5000 });
    return response.data;
  } catch (error) {
    console.error('Health check failed:', error.message);
    if (error.code === 'ECONNABORTED') {
      return { status: 'timeout', message: 'Connection timeout' };
    }
    if (error.response) {
      return error.response.data;
    }
    return null;
  }
};

export default api;