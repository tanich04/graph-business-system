import React, { useEffect, useRef, useState } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import api from '../services/api';
import '../styles/GraphCanvas.css';

const GraphCanvas = ({ onNodeClick, highlightedNodes = [], selectedNode, onNodeSelect }) => {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [hoveredNode, setHoveredNode] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [expandedTypes, setExpandedTypes] = useState(new Set());
  const fgRef = useRef();

  // 🎨 COLORS
  const COLORS = {
    SalesOrder: '#3B82F6',
    SalesOrderItem: '#60A5FA',
    BillingDocument: '#10B981',
    BillingDocumentItem: '#34D399',
    Product: '#EF4444',
    Customer: '#8B5CF6',
    Delivery: '#F59E0B',
    DeliveryItem: '#FBBF24',
    Payment: '#14B8A6',
    JournalEntry: '#EC489A',
    Plant: '#06B6D4',
    Address: '#84CC16'
  };

  // 🎨 COLOR FIX (CRITICAL)
  const getNodeColor = (node) => {
    if (highlightedNodes?.includes(node.id)) return '#FF3B3B';

    const type = node.type || node.original_type;

    return COLORS[type] || '#4B5563';
  };

  useEffect(() => {
    if (fgRef.current) {
        fgRef.current.d3Force('charge').strength(-50);   // 🔥 spread more
        fgRef.current.d3Force('link').distance(60);
        fgRef.current.d3Force('center').strength(0.2);
    }
    }, [graphData]);

  const getNodeSize = (node) => {
    if (highlightedNodes?.includes(node.id)) return 9;
    if (node.isSummary) return 8;

    const sizes = {
      SalesOrder: 6,
      Customer: 6,
      Product: 5,
      BillingDocument: 6,
      Delivery: 5
    };

    return sizes[node.type] || 5;
  };

  const getLabel = (node) => {
    if (node.isSummary) return node.original_type;

    if (node.type === 'Customer') {
      return node.properties?.businessPartnerName || node.id.slice(-6);
    }

    if (node.type === 'Product') {
      return node.properties?.productDescription || node.id.slice(-6);
    }

    return `${node.type}:${node.id.slice(-5)}`;
  };

  // 🔥 LOAD OVERVIEW (FIXED TYPE MAPPING)
  const loadOverview = async () => {
    setLoading(true);
    try {
      const res = await api.get('/api/graph/overview');
      const data = res.data;

      const nodes = data.nodes.map(n => ({
        id: n.id,
        type: n.original_type || n.type,   // ✅ FIX
        original_type: n.original_type,
        name: n.original_type,
        count: n.count,
        properties: n.properties || {},
        isSummary: true
      }));

      const links = data.links.map(l => ({
        source: l.source,
        target: l.target,
        type: l.type
      }));

      setGraphData({ nodes, links });

      setTimeout(() => fgRef.current?.zoomToFit(500), 100);
    } catch (err) {
      console.error('Overview load failed:', err);
    } finally {
      setLoading(false);
    }
  };

  // 🔥 EXPAND SUMMARY (FIXED)
  const expandSummaryNode = async (node) => {
    const nodeType = node.original_type || node.type;

    if (!nodeType || expandedTypes.has(nodeType)) return;

    setLoadingMore(true);

    try {
      const res = await api.get('/api/graph/nodes', {
        params: { node_type: nodeType, limit: 15 }
      });

      const newNodes = res.data.map(n => ({
        id: n.id,
        type: nodeType,
        properties: n.properties || {},
        isSummary: false
      }));

      setGraphData(prev => ({
        nodes: [...prev.nodes, ...newNodes],
        links: prev.links
      }));

      setExpandedTypes(prev => new Set(prev).add(nodeType));

      setTimeout(() => fgRef.current?.zoomToFit(800), 200);

    } catch (err) {
      console.error('Expand failed:', err);
    } finally {
      setLoadingMore(false);
    }
  };

  // 🔥 LOAD NEIGHBORS
  const loadNodeNeighbors = async (node) => {
    if (node.isSummary) return;

    setLoadingMore(true);

    try {
      const res = await api.get(`/api/graph/neighbors/${encodeURIComponent(node.id)}`);

      const newNodes = res.data.nodes || [];
      const newEdges = res.data.edges || [];

      const existingIds = new Set(graphData.nodes.map(n => n.id));

      const nodesToAdd = newNodes
        .filter(n => !existingIds.has(n.id))
        .map(n => ({
          id: n.id,
          type: n.label || 'Unknown',
          properties: n.properties || {},
          isSummary: false
        }));

      const linksToAdd = newEdges.map(e => ({
        source: e.source,
        target: e.target,
        type: e.type
      }));

      setGraphData(prev => ({
        nodes: [...prev.nodes, ...nodesToAdd],
        links: [...prev.links, ...linksToAdd]
      }));

    } catch (err) {
      console.error('Neighbors load failed:', err);
    } finally {
      setLoadingMore(false);
    }
  };

  const handleNodeClick = async (node) => {
    onNodeSelect?.({ ...node }); 
    onNodeClick?.(node);

    if (node.isSummary) {
      await expandSummaryNode(node);
    } else {
      await loadNodeNeighbors(node);
    }
  };

  useEffect(() => {
    loadOverview();
  }, []);

  if (loading) {
    return <div className="graph-loading">Loading graph...</div>;
  }

  return (
    <div className="graph-container">

      <ForceGraph2D
        ref={fgRef}
        graphData={graphData}

        backgroundColor="#F1F5F9"

        nodeVal={getNodeSize}
        nodeColor={getNodeColor}

        linkColor={() => '#94A3B8'}
        linkWidth={1.5}

        linkDirectionalArrowLength={4}
        linkDirectionalArrowRelPos={1}

        onNodeClick={handleNodeClick}
        onNodeHover={setHoveredNode}

        cooldownTicks={60}
        d3VelocityDecay={0.35}

        width={window.innerWidth * 0.68}
        height={window.innerHeight - 100}

        // 🔥 FINAL FIXED DRAW
        nodeCanvasObject={(node, ctx, scale) => {
          const size = getNodeSize(node);
          const color = getNodeColor(node);

          ctx.shadowBlur = 6;
          ctx.shadowColor = color;

          ctx.beginPath();
          ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
          ctx.fillStyle = color;
          ctx.fill();

          ctx.strokeStyle = '#fff';
          ctx.lineWidth = 2 / scale;
          ctx.stroke();

          ctx.shadowBlur = 0;

          if (node === hoveredNode) {
            ctx.font = `${12 / scale}px Arial`;
            ctx.fillStyle = '#111';
            ctx.textAlign = 'center';
            ctx.fillText(getLabel(node), node.x, node.y - size - 6);
          }
        }}
      />

    </div>
  );
};

export default GraphCanvas;