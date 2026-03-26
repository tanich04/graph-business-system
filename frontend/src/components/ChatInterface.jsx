import React, { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import { FiSend, FiLoader, FiTrash2, FiCopy, FiCheck } from 'react-icons/fi';
import { sendQueryStream } from '../services/api';
import '../styles/ChatInterface.css';
import userAvatar from '../assets/user.jpg';
import botAvatar from '../assets/bot.png';

const ChatInterface = ({ onNodesHighlighted }) => {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [streamingMessage, setStreamingMessage] = useState(null);
  const [copiedId, setCopiedId] = useState(null);
  const [sessionId] = useState(() => `session_${Date.now()}`);
  const messagesEndRef = useRef(null);
  const textareaRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, streamingMessage]);

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight, 120) + 'px';
    }
  }, [input]);

  const handleCopy = (text, id) => {
    navigator.clipboard.writeText(text);
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  const handleSend = async () => {
    if (!input.trim() || loading) return;

    const userMessage = {
      id: Date.now(),
      role: 'user',
      content: input,
      timestamp: new Date(),
    };
    
    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setLoading(true);
    setStreamingMessage({ id: Date.now() + 1, role: 'assistant', content: '', query: null });

    try {
      let assistantContent = '';
      let queryUsed = null;
      let nodesMentioned = [];

      await sendQueryStream(input, sessionId, (data) => {
        switch (data.type) {
          case 'query_chunk':
            // Collect query chunks
            if (!queryUsed) queryUsed = '';
            queryUsed += data.content;
            break;
            
          case 'nodes':
            nodesMentioned = data.content;
            if (onNodesHighlighted) {
              onNodesHighlighted(nodesMentioned);
            }
            break;
            
          case 'response_chunk':
            assistantContent += data.content;
            setStreamingMessage({
              id: Date.now() + 1,
              role: 'assistant',
              content: assistantContent,
              query: queryUsed,
              nodes: nodesMentioned,
              streaming: true
            });
            break;
            
          case 'done':
            // Final message
            setStreamingMessage(null);
            setMessages(prev => [...prev, {
              id: Date.now(),
              role: 'assistant',
              content: assistantContent,
              query: queryUsed,
              nodes: nodesMentioned,
              timestamp: new Date()
            }]);
            setLoading(false);
            break;
            
          case 'error':
            setStreamingMessage(null);
            setMessages(prev => [...prev, {
              id: Date.now(),
              role: 'assistant',
              content: `❌ Error: ${data.content}`,
              isError: true,
              timestamp: new Date()
            }]);
            setLoading(false);
            break;
        }
      });
    } catch (error) {
      console.error('Failed to send query:', error);
      setMessages(prev => [...prev, {
        id: Date.now(),
        role: 'assistant',
        content: 'Sorry, I encountered an error. Please try again.',
        isError: true,
        timestamp: new Date()
      }]);
      setLoading(false);
      setStreamingMessage(null);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const clearChat = () => {
    setMessages([]);
    setStreamingMessage(null);
  };

  const formatTime = (date) => {
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  };

  return (
    <div className="chat-container">
      <div className="chat-header">
        <div className="header-left">
          <h3>💬 Business Query Assistant</h3>
          <span className="status-badge">
            {loading ? 'Processing...' : 'Ready'}
          </span>
        </div>
        <button onClick={clearChat} className="clear-button" title="Clear chat">
          <FiTrash2 />
        </button>
      </div>
      
      <div className="messages-container">
        {messages.length === 0 && !streamingMessage && (
          <div className="welcome-message">
            <div className="welcome-icon">🤖</div>
            <h4>Welcome to Graph Business Intelligence</h4>
            <p>Ask me anything about your business data:</p>
            <div className="example-queries">
              <div className="example-category">
                <strong>📊 Analytics:</strong>
                <ul>
                  <li>"Which products have the highest billing volume?"</li>
                  <li>"What's the total revenue from billing documents?"</li>
                  <li>"Show me top 5 customers by order value"</li>
                </ul>
              </div>
              <div className="example-category">
                <strong>🔍 Operations:</strong>
                <ul>
                  <li>"Find sales orders that were delivered but not billed"</li>
                  <li>"Show me incomplete orders"</li>
                  <li>"Trace the full flow for billing document 90504248"</li>
                </ul>
              </div>
            </div>
          </div>
        )}
        
        {messages.map((msg) => (
          <div key={msg.id} className={`message ${msg.role}`}>
            <div className="message-avatar">
              <img 
                src={msg.role === 'user' ? userAvatar : botAvatar} 
                alt="avatar" 
              />
            </div>
            <div className="message-content-wrapper">
              <div className={`message-bubble ${msg.isError ? 'error' : ''}`}>
                <ReactMarkdown>{msg.content}</ReactMarkdown>
                {msg.query && (
                  <details className="query-details">
                    <summary>🔍 View Cypher Query</summary>
                    <pre><code>{msg.query}</code></pre>
                    <button 
                      className="copy-button"
                      onClick={() => handleCopy(msg.query, `query-${msg.id}`)}
                    >
                      {copiedId === `query-${msg.id}` ? <FiCheck /> : <FiCopy />}
                      {copiedId === `query-${msg.id}` ? ' Copied!' : ' Copy'}
                    </button>
                  </details>
                )}
              </div>
              <div className="message-time">{formatTime(msg.timestamp)}</div>
            </div>
          </div>
        ))}
        
        {streamingMessage && (
          <div className="message assistant streaming">
            <div className="message-avatar">🤖</div>
            <div className="message-content-wrapper">
              <div className="message-bubble">
                <ReactMarkdown>{streamingMessage.content}</ReactMarkdown>
                {streamingMessage.query && (
                  <details className="query-details">
                    <summary>🔍 View Cypher Query (generating...)</summary>
                    <pre><code>{streamingMessage.query}</code></pre>
                  </details>
                )}
                <div className="typing-indicator">
                  <span></span>
                  <span></span>
                  <span></span>
                </div>
              </div>
            </div>
          </div>
        )}
        
        {loading && !streamingMessage && (
          <div className="message assistant loading">
            <div className="message-avatar">🤖</div>
            <div className="message-content-wrapper">
              <div className="message-bubble">
                <FiLoader className="spinner" />
                <span>Thinking...</span>
              </div>
            </div>
          </div>
        )}
        
        <div ref={messagesEndRef} />
      </div>
      
      <div className="input-container">
        <div className="input-wrapper">
          <textarea
            ref={textareaRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="I got three magical words..Ask me anything..."
            rows={1}
            disabled={loading}
          />
          <button 
            onClick={handleSend} 
            disabled={!input.trim() || loading}
            className={`send-button ${!input.trim() || loading ? 'disabled' : ''}`}
          >
            {loading ? <FiLoader className="spinner" /> : <FiSend />}
          </button>
        </div>
        <div className="input-hint">
          Press Enter to send, Shift+Enter for new line
        </div>
      </div>
    </div>
  );
};

export default ChatInterface;