import React from 'react';
import { getDataFormat } from './util.js';

function MetadataOverview({ metadataOptions, searchTerm, setSearchTerm, selectedMetadata, setSelectedMetadata }) {


    const handleCardClick = metadata => {
        if (metadata === selectedMetadata) {
            setSelectedMetadata(null);
            return;
        }
        setSelectedMetadata(metadata);
    };

    const filteredOptions = metadataOptions.filter(metadata =>
        metadata.schema_name.toLowerCase().includes(searchTerm.toLowerCase())
    );

    const getCardStyle = schemaName => {
        if (!selectedMetadata) return {};
        return schemaName === selectedMetadata.schema_name ? { backgroundColor: '#f0f0f0' } : {};
    }
    
    return (
        <div className="cards-section">
            <div className="search-container">
                <input
                    type="text"
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    placeholder="Search schemas..."
                />
            </div>
            <div className="cards-container">
                <div className="cards">
                    {filteredOptions.map(metadata => (
                        <div key={metadata.schema_name} className="card" onClick={() => handleCardClick(metadata)} style={getCardStyle(metadata.schema_name)}>
                            <div>{metadata.schema_name}</div>
                            <div>{getDataFormat(metadata)}</div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}

export default MetadataOverview;
