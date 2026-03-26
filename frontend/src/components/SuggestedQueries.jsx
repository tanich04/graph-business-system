import React, { useState, useEffect } from 'react';
import { getSuggestions } from '../services/api';
import '../styles/SuggestedQueries.css';

const SuggestedQueries = ({ onQuerySelect }) => {
  const [suggestions, setSuggestions] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadSuggestions();
  }, []);

  const loadSuggestions = async () => {
    try {
      const data = await getSuggestions();
      setSuggestions(data.suggestions);
    } catch (error) {
      console.error('Failed to load suggestions:', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) return null;

  return (
    <div className="suggestions-container">
      <h4>Suggested Questions</h4>
      <div className="suggestions-list">
        {suggestions.slice(0, 6).map((suggestion, idx) => (
          <button
            key={idx}
            className="suggestion-chip"
            onClick={() => onQuerySelect(suggestion)}
          >
            {suggestion}
          </button>
        ))}
      </div>
    </div>
  );
};

export default SuggestedQueries;