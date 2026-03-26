import React from 'react';
import {
  Box,
  Typography,
  Chip,
  Divider,
  Paper,
  Table,
  TableBody,
  TableRow,
  TableCell
} from '@mui/material';

const NodeInfoPanel = ({ node }) => {
  if (!node) {
    return (
      <Box sx={{ p: 2, textAlign: 'center', color: 'text.secondary' }}>
        <Typography variant="body2">
          Click on any node to view details
        </Typography>
      </Box>
    );
  }

  const formatValue = (value) => {
    if (value === null || value === undefined) return 'N/A';
    if (typeof value === 'boolean') return value ? 'Yes' : 'No';
    if (typeof value === 'number') {
      if (value > 1000) return `₹${value.toLocaleString()}`;
      return value;
    }
    if (typeof value === 'string' && value.length > 50) {
      return value.substring(0, 50) + '...';
    }
    return value;
  };

  const getImportantProperties = () => {
    const important = [];
    const skipKeys = ['element_id', 'id'];
    
    for (const [key, value] of Object.entries(node.properties)) {
      if (skipKeys.includes(key)) continue;
      if (value !== null && value !== undefined && value !== '') {
        important.push({ key, value });
      }
    }
    return important.slice(0, 15); // Limit to 15 properties
  };

  return (
    <Box sx={{ p: 2 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Typography variant="subtitle1" sx={{ fontWeight: 'bold' }}>
          {node.type}
        </Typography>
        <Chip 
          label={node.id} 
          size="small" 
          color="primary" 
          variant="outlined"
        />
      </Box>
      
      <Divider sx={{ my: 1.5 }} />
      
      <Typography variant="subtitle2" gutterBottom>
        Properties
      </Typography>
      
      <Table size="small">
        <TableBody>
          {getImportantProperties().map(({ key, value }) => (
            <TableRow key={key}>
              <TableCell 
                component="th" 
                scope="row"
                sx={{ 
                  borderBottom: 'none',
                  fontWeight: 'bold',
                  color: 'text.secondary'
                }}
              >
                {key}
              </TableCell>
              <TableCell 
                sx={{ borderBottom: 'none' }}
              >
                {formatValue(value)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      
      {node.relationships && node.relationships.length > 0 && (
        <>
          <Divider sx={{ my: 1.5 }} />
          <Typography variant="subtitle2" gutterBottom>
            Relationships ({node.relationships.length})
          </Typography>
          <Box sx={{ maxHeight: 200, overflow: 'auto' }}>
            {node.relationships.slice(0, 10).map((rel, idx) => (
              <Chip
                key={idx}
                label={`${rel.relationship_type} → ${rel.connected_type}`}
                size="small"
                variant="outlined"
                sx={{ m: 0.5 }}
              />
            ))}
            {node.relationships.length > 10 && (
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
                + {node.relationships.length - 10} more relationships
              </Typography>
            )}
          </Box>
        </>
      )}
    </Box>
  );
};

export default NodeInfoPanel;