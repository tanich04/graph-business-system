import React, { useState } from 'react';
import { FiSearch } from 'react-icons/fi';
import { searchNodes } from '../services/api';
import '../styles/SearchBar.css';

const SearchBar = ({ onSearchResult, nodeTypes }) => {
  const [query, setQuery] = useState('');
  const [searching, setSearching] = useState(false);
  const [results, setResults] = useState([]);
  const [selectedType, setSelectedType] = useState('');

  const handleSearch = async () => {
    if (!query.trim()) return;
    
    setSearching(true);
    try {
      const results = await searchNodes(query, selectedType || null, 20);
      setResults(results);
      onSearchResult?.(results);
    } catch (error) {
      console.error('Search failed:', error);
    } finally {
      setSearching(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  return (
    <div className="search-container">
      <div className="search-input-wrapper">
        <FiSearch className="search-icon" />
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyPress={handleKeyPress}
          placeholder="Search nodes..."
          className="search-input"
        />
        <button onClick={handleSearch} disabled={searching} className="search-button">
          {searching ? '...' : 'Search'}
        </button>
      </div>
      
      {nodeTypes && (
        <select 
          value={selectedType} 
          onChange={(e) => setSelectedType(e.target.value)}
          className="type-filter"
        >
          <option value="">All Types</option>
          {nodeTypes.map(type => (
            <option key={type} value={type}>{type}</option>
          ))}
        </select>
      )}
      
      {results.length > 0 && (
        <div className="search-results">
          {results.map((result, idx) => (
            <div key={idx} className="search-result-item">
              <span className="result-type">{result.type}</span>
              <span className="result-id">{result.id}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default SearchBar;