import React from 'react';
import { getDataFormat } from './util.js';

function MetadataOverview({ metadataOptions, searchTerm, setSearchTerm, setSelectedMetadata }) {


    const handleCardClick = metadata => {
        setSelectedMetadata(metadata);
    };

    const filteredOptions = metadataOptions.filter(metadata =>
        metadata.schema_name.toLowerCase().includes(searchTerm.toLowerCase())
    );

    return (
        <div className="section">
            <h3>Overview</h3>
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
                        <div key={metadata.schema_name} className="card" onClick={() => handleCardClick(metadata)}>
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
