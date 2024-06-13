import React, { useContext, useState, useEffect } from 'react';
import { DBContext } from './DBContext';
import './MetadataTab.css';

function MetadataTab() {
    const { swaggerOptions, metadataOptions } = useContext(DBContext);
    const [searchTerm, setSearchTerm] = useState('');
    const [filteredOptions, setFilteredOptions] = useState(swaggerOptions);
    const [selectedMetadata, setSelectedMetadata] = useState(null);

    useEffect(() => {
        const filtered = swaggerOptions.filter(option =>
            option.toLowerCase().includes(searchTerm.toLowerCase())
        );
        setFilteredOptions(filtered);
    }, [searchTerm, swaggerOptions]);

    const getDataFormat = (metadata) => {
        const hasTemporal = metadata.temporal_start || metadata.temporal_end;
        const hasSpatial = metadata.bbox_min_lat || metadata.bbox_max_lat || metadata.bbox_min_lon || metadata.bbox_max_lon;
        if (hasTemporal && hasSpatial) return "Type: Temporal & Spatial";
        if (hasTemporal) return "Type: Temporal";
        if (hasSpatial) return "Type: Spatial";
        return "Type: Standard";
    };

    const handleCardClick = (metadata) => {
        setSelectedMetadata(metadata);
    };

    return (
        <div className="metadata-tab">
            <h2>Metadata</h2>
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
                        {metadataOptions.map(metadata => (
                            <div key={metadata.schema_name} className="card" onClick={() => handleCardClick(metadata)}>
                                <div>{metadata.schema_name}</div>
                                <div>{getDataFormat(metadata)}</div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
            {selectedMetadata && (
                <div className="section detail-view">
                    <h3>Details for {selectedMetadata.schema_name}</h3>
                    <table className="metadata-table">
                        <tbody>
                            <tr><td>Schema Name</td><td>{selectedMetadata.schema_name}</td></tr>
                            <tr><td>Crawl Date</td><td>{selectedMetadata.crawl_date || 'N/A'}</td></tr>
                            <tr><td>Data Date</td><td>{selectedMetadata.data_date || 'N/A'}</td></tr>
                            <tr><td>Data Source</td><td>{selectedMetadata.data_source || 'N/A'}</td></tr>
                            <tr><td>Licence</td><td>{selectedMetadata.licence || 'N/A'}</td></tr>
                            <tr><td>Description</td><td>{selectedMetadata.description || 'N/A'}</td></tr>
                            <tr><td>Contact</td><td>{selectedMetadata.contact || 'N/A'}</td></tr>
                            <tr><td>Tables</td><td>{selectedMetadata.tables || 'N/A'}</td></tr>
                            <tr><td>Size</td><td>{selectedMetadata.size ? `${selectedMetadata.size} bytes` : 'N/A'}</td></tr>
                            <tr><td>Type</td><td>{getDataFormat(selectedMetadata)}</td></tr>
                        </tbody>
                    </table>
                </div>
            )}

            <div className="section">
                <h3>Availability Timeline</h3>
                <p>Timeline content goes here.</p>
            </div>
            <div className="section">
                <h3>Availability Map</h3>
                <p>Map content goes here.</p>
            </div>
        </div>
    );
}

export default MetadataTab;
