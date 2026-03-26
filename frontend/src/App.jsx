import React, { useState, useEffect } from 'react';
import GraphCanvas from './components/GraphCanvas';
import ChatInterface from './components/ChatInterface';
import NodeDetails from './components/NodeDetails';
import SuggestedQueries from './components/SuggestedQueries';
import SearchBar from './components/SearchBar';
import { getSchema, getStatistics, healthCheck } from './services/api';
import './styles/App.css';
import logo from './assets/dodge_ai_logo.png';

function App() {
  const [selectedNode, setSelectedNode] = useState(null);
  const [highlightedNodes, setHighlightedNodes] = useState([]);
  const [nodeTypes, setNodeTypes] = useState([]);
  const [statistics, setStatistics] = useState(null);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [backendStatus, setBackendStatus] = useState('checking');
  const [showDetails, setShowDetails] = useState(false);
  const [retryCount, setRetryCount] = useState(0);

  useEffect(() => {
    checkBackendHealth();
  }, [retryCount]);

  const checkBackendHealth = async () => {
    setBackendStatus('checking');
    try {
      const health = await healthCheck();
      console.log('Health check response:', health);
      
      if (health && health.status === 'healthy') {
        setBackendStatus('connected');
        // Load data only after backend is confirmed healthy
        loadSchema();
        loadStatistics();
      } else if (health && health.status === 'degraded') {
        setBackendStatus('degraded');
        console.warn('Backend is degraded:', health.services);
        // Still try to load data
        loadSchema();
        loadStatistics();
      } else {
        setBackendStatus('error');
      }
    } catch (error) {
      console.error('Backend health check failed:', error);
      setBackendStatus('error');
    }
  };

  const loadSchema = async () => {
    try {
      const schema = await getSchema();
      setNodeTypes(schema.node_types || []);
    } catch (error) {
      console.error('Failed to load schema:', error);
    }
  };

  const loadStatistics = async () => {
    try {
      const stats = await getStatistics();
      setStatistics(stats);
    } catch (error) {
      console.error('Failed to load statistics:', error);
    }
  };

  const handleNodeClick = (node) => {
    setSelectedNode(node);
    setShowDetails(true);
  };

  const handleNodeSelect = (node) => {
    setSelectedNode(node);
    setShowDetails(true);
  };

  const handleCloseDetails = () => {
    setShowDetails(false);
    setSelectedNode(null);
  };

  const handleNodesHighlighted = (nodeIds) => {
    setHighlightedNodes(nodeIds);
    setTimeout(() => {
      setHighlightedNodes([]);
    }, 5000);
  };

  const handleQuerySelect = (query) => {
    const textarea = document.querySelector('.input-container textarea');
    if (textarea) {
      textarea.value = query;
      textarea.dispatchEvent(new Event('input', { bubbles: true }));
    }
  };

  const handleRetry = () => {
    setRetryCount(prev => prev + 1);
  };

  // Loading state
  if (backendStatus === 'checking') {
    return (
      <div className="app loading">
        <div className="loading-container">
          <div className="spinner-large"></div>
          <h2>Connecting to Backend...</h2>
          <p>Please wait while we establish connection to the server</p>
        </div>
      </div>
    );
  }

  // Error state
  if (backendStatus === 'error') {
    return (
      <div className="app error">
        <div className="error-container">
          <div className="error-icon">⚠️</div>
          <h2>Backend Connection Error</h2>
          <p>Unable to connect to the backend server at http://localhost:8000</p>
          <p className="error-details">Please make sure the backend is running:</p>
          <code>cd backend && python run.py</code>
          <div className="error-actions">
            <button onClick={handleRetry} className="retry-button">
              Retry Connection
            </button>
            <a href="http://localhost:8000/docs" target="_blank" rel="noopener noreferrer" className="docs-link">
              Check API Docs
            </a>
          </div>
          <div className="debug-info">
            <details>
              <summary>Debug Information</summary>
              <pre>
                {`Backend URL: http://localhost:8000
Health Check: ${backendStatus}
Time: ${new Date().toLocaleString()}`}
              </pre>
            </details>
          </div>
        </div>
      </div>
    );
  }

  // Degraded state (backend partially working)
  if (backendStatus === 'degraded') {
    return (
      <div className="app degraded">
        <div className="degraded-banner">
          ⚠️ Backend is running in degraded mode. Some features may be limited.
        </div>
        <div className="app-header">
          <h1>📊 Graph Business Intelligence</h1>
          <div className="stats-badge">
            {statistics ? (
              `${statistics.total_nodes?.toLocaleString()} nodes | ${statistics.total_relationships?.toLocaleString()} relationships`
            ) : (
              'Loading stats...'
            )}
          </div>
        </div>
        
        <div className="app-content">
          <div className="graph-section">
            <div className="graph-controls">
              <SearchBar 
                onSearchResult={(results) => {
                  const nodeIds = results.map(r => r.id);
                  handleNodesHighlighted(nodeIds);
                }}
                nodeTypes={nodeTypes}
              />
            </div>
            <GraphCanvas 
              onNodeClick={handleNodeClick}
              onNodeSelect={handleNodeSelect}
              highlightedNodes={highlightedNodes}
              selectedNode={selectedNode}
            />
          </div>
          
          <div className={`sidebar ${sidebarOpen ? 'open' : 'closed'}`}>
            <button 
              className="sidebar-toggle"
              onClick={() => setSidebarOpen(!sidebarOpen)}
            >
              {sidebarOpen ? '→' : '←'}
            </button>
            
            {sidebarOpen && (
              <div className="sidebar-content">
                {showDetails && selectedNode ? (
                  <NodeDetails 
                    node={selectedNode} 
                    onClose={handleCloseDetails}
                    onExpand={(node) => {
                      console.log('Expand node:', node);
                    }}
                  />
                ) : (
                  <>
                    <SuggestedQueries onQuerySelect={handleQuerySelect} />
                    <ChatInterface onNodesHighlighted={handleNodesHighlighted} />
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  // Healthy state - main app
  return (
    <div className="app">
      <div className="app-header">
        <h1 className="app-title">
          <img src={logo} alt="Dodge AI Logo" />
          डॉज AI Query Bot
        </h1>
        <div className="stats-badge">
          {statistics ? (
            `${statistics.total_nodes?.toLocaleString()} nodes | ${statistics.total_relationships?.toLocaleString()} relationships`
          ) : (
            'Loading stats...'
          )}
        </div>
      </div>
      
      <div className="app-content">
        <div className="graph-section">
          <div className="graph-controls">
            <SearchBar 
              onSearchResult={(results) => {
                const nodeIds = results.map(r => r.id);
                handleNodesHighlighted(nodeIds);
              }}
              nodeTypes={nodeTypes}
            />
          </div>
          <GraphCanvas 
            onNodeClick={handleNodeClick}
            onNodeSelect={handleNodeSelect}
            highlightedNodes={highlightedNodes}
            selectedNode={selectedNode}
          />
        </div>
        
        <div className={`sidebar ${sidebarOpen ? 'open' : 'closed'}`}>
          <button 
            className="sidebar-toggle"
            onClick={() => setSidebarOpen(!sidebarOpen)}
          >
            {sidebarOpen ? '→' : '←'}
          </button>
          
          {sidebarOpen && (
            <div className="sidebar-content">
              {showDetails && selectedNode ? (
                <NodeDetails 
                  node={selectedNode} 
                  onClose={handleCloseDetails}
                  onExpand={(node) => {
                    console.log('Expand node:', node);
                  }}
                />
              ) : (
                <>
                  <SuggestedQueries onQuerySelect={handleQuerySelect} />
                  <ChatInterface onNodesHighlighted={handleNodesHighlighted} />
                </>
              )}
            </div>
          )}
        </div>
      </div>
      
      {selectedNode && !showDetails && (
        <NodeDetails 
          node={selectedNode} 
          onClose={handleCloseDetails}
          onExpand={(node) => {
            console.log('Expand node:', node);
          }}
        />
      )}
    </div>
  );
}

export default App;