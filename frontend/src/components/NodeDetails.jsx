import React, { useState, useEffect } from 'react';
import api from '../services/api';
import '../styles/NodeDetails.css';

const NodeDetails = ({ node, onClose, onExpand }) => {
  const [loading, setLoading] = useState(false);
  const [actualNode, setActualNode] = useState(node);
  const [relationships, setRelationships] = useState([]);
  const [sampleNodes, setSampleNodes] = useState([]);

  // Don't try to fetch relationships for summary nodes
  useEffect(() => {
    if (node) {
      if (node.isSummary || node.type === 'Summary') {
        // For summary nodes, just show count and load sample nodes
        loadSampleNodesForType(node.original_type || node.type);
      } else if (node.id && !node.id.toString().startsWith('summary_')) {
        // Only fetch relationships for actual nodes (not summary nodes)
        loadNodeRelationships();
      }
    }
  }, [node]);

  const loadSampleNodesForType = async (nodeType) => {
    if (!nodeType || nodeType === 'Summary') return;
    
    setLoading(true);
    try {
      const response = await api.get('/api/graph/nodes', {
        params: { 
          node_type: nodeType, 
          limit: 5,
          sort_by: 'creationDate',
          sort_order: 'desc'
        }
      });
      
      const nodes = response.data;
      setSampleNodes(nodes);
      
      // Update actual node with sample properties if available
      if (nodes && nodes.length > 0) {
        setActualNode({
          ...node,
          properties: nodes[0].properties || {},
          sampleNodes: nodes
        });
      }
    } catch (error) {
      console.error('Failed to load sample nodes:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadNodeRelationships = async () => {
    if (!node.id || node.id.toString().startsWith('summary_')) return;
    
    setLoading(true);
    try {
      // Only fetch relationships for actual node IDs
      const response = await api.get(`/api/graph/node/${encodeURIComponent(node.id)}`);
      if (response.data && response.data.relationships) {
        setRelationships(response.data.relationships);
      }
    } catch (error) {
      console.error('Failed to load relationships:', error);
      // Don't show error for 404 on summary nodes
      if (error.response?.status !== 404) {
        console.error('Error details:', error.response?.data);
      }
    } finally {
      setLoading(false);
    }
  };

  const formatValue = (value) => {
    if (value === null || value === undefined) return 'N/A';
    if (typeof value === 'object') return JSON.stringify(value).slice(0, 100);
    if (typeof value === 'number') return value.toLocaleString();
    return String(value);
  };

  const getTypeDisplay = () => {
    if (actualNode.isSummary || actualNode.type === 'Summary') {
      return actualNode.original_type || actualNode.type || 'Summary';
    }
    return actualNode.type || 'Node';
  };

  const getNodeColor = () => {
    const colors = {
      'SalesOrder': '#3B82F6',
      'SalesOrderItem': '#60A5FA',
      'BillingDocument': '#10B981',
      'BillingDocumentItem': '#34D399',
      'Product': '#EF4444',
      'Customer': '#8B5CF6',
      'Delivery': '#F59E0B',
      'DeliveryItem': '#FBBF24',
      'Payment': '#14B8A6',
      'JournalEntry': '#EC489A'
    };
    const nodeType = getTypeDisplay();
    return colors[nodeType] || '#9CA3AF';
  };

  const getRelationshipsDisplay = () => {
    if (loading) {
      return <p className="loading-text">Loading relationships...</p>;
    }
    
    if (actualNode.isSummary || actualNode.type === 'Summary') {
      return (
        <div className="no-relationships">
          <p>This is a summary node representing {actualNode.count || 'many'} {getTypeDisplay()} nodes.</p>
          <p>Click on "Expand" in the graph to load actual nodes.</p>
        </div>
      );
    }
    
    if (!relationships || relationships.length === 0) {
      return (
        <div className="no-relationships">
          <p>No relationships loaded for this node.</p>
          <button 
            className="load-button"
            onClick={() => onExpand && onExpand(actualNode)}
          >
            Load Neighbors →
          </button>
        </div>
      );
    }
    
    return (
      <div className="relationships-list">
        {relationships.slice(0, 10).map((rel, idx) => (
          <div key={idx} className="relationship-item">
            <span className="relationship-type">{rel.relationship_type}</span>
            <span className="relationship-target">
              → {rel.connected_type}: {
                rel.salesOrder || 
                rel.billingDocument || 
                rel.deliveryDocument || 
                rel.product || 
                rel.customer || 
                'Unknown'
              }
            </span>
          </div>
        ))}
        {relationships.length > 10 && (
          <p className="more-info">... and {relationships.length - 10} more</p>
        )}
      </div>
    );
  };

  const getPropertiesDisplay = () => {
    if (!actualNode.properties || Object.keys(actualNode.properties).length === 0) {
      return <p className="no-data">No properties available</p>;
    }
    
    const importantProps = [
      'businessPartnerName', 'productDescription', 'totalNetAmount', 
      'transactionCurrency', 'creationDate', 'overallDeliveryStatus',
      'billingDocumentType', 'salesOrderType', 'shippingPoint',
      'actualDeliveryQuantity', 'deliveryQuantityUnit', 'plant',
      'material', 'billingQuantity', 'netAmount'
    ];
    
    const sortedProps = Object.entries(actualNode.properties)
      .filter(([_, v]) => v !== null && v !== undefined && v !== '')
      .sort(([a], [b]) => {
        const aImportance = importantProps.includes(a) ? 0 : 1;
        const bImportance = importantProps.includes(b) ? 0 : 1;
        return aImportance - bImportance;
      });
    
    return (
      <div className="properties-grid">
        {sortedProps.slice(0, 15).map(([key, value]) => (
          <div key={key} className="property-item">
            <div className="property-key">{key}:</div>
            <div className="property-value">{formatValue(value)}</div>
          </div>
        ))}
        {sortedProps.length > 15 && (
          <p className="more-info">... and {sortedProps.length - 15} more properties</p>
        )}
      </div>
    );
  };

  const getSampleNodesDisplay = () => {
    if (sampleNodes && sampleNodes.length > 0) {
      return (
        <div className="sample-nodes">
          <h4>Sample {getTypeDisplay()} Nodes</h4>
          <div className="sample-list">
            {sampleNodes.slice(0, 5).map((sample, idx) => (
              <div key={idx} className="sample-item">
                <span className="sample-id">{sample.id}</span>
                {sample.properties?.totalNetAmount && (
                  <span className="sample-amount">
                    ₹{parseFloat(sample.properties.totalNetAmount).toLocaleString()}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      );
    }
    return null;
  };

  const handleExpand = () => {
    if (onExpand) {
      onExpand(actualNode);
    }
  };

  return (
    <div className="node-details-sidebar">
      <div className="node-details-header" style={{ borderLeftColor: getNodeColor() }}>
        <div className="header-info">
          <div className="node-type-badge" style={{ backgroundColor: getNodeColor() }}>
            {getTypeDisplay()}
          </div>
          <h3>{actualNode.id}</h3>
          {(actualNode.count || actualNode.totalCount) && (
            <div className="total-count-badge">
              Total: {(actualNode.count || actualNode.totalCount).toLocaleString()} nodes
            </div>
          )}
        </div>
        <button className="close-button" onClick={onClose}>×</button>
      </div>
      
      <div className="node-details-content">
        {(actualNode.isSummary || actualNode.type === 'Summary') && (
          <div className="summary-info">
            <div className="info-card">
              <span className="info-label">Total Nodes</span>
              <span className="info-value">{(actualNode.count || actualNode.totalCount || '?').toLocaleString()}</span>
            </div>
            <button className="expand-button" onClick={handleExpand}>
              Expand {getTypeDisplay()} Nodes →
            </button>
          </div>
        )}
        
        {getSampleNodesDisplay()}
        
        <div className="detail-section">
          <h4>Properties</h4>
          {getPropertiesDisplay()}
        </div>
        
        <div className="detail-section">
          <h4>Relationships</h4>
          {getRelationshipsDisplay()}
        </div>
        
        {!actualNode.isSummary && actualNode.type !== 'Summary' && (
          <div className="detail-section">
            <h4>Actions</h4>
            <button className="action-button" onClick={handleExpand}>
              Expand Neighbors
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default NodeDetails;