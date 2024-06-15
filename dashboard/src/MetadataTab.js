import React, { useContext, useState } from 'react';
import { DBContext } from './DBContext';
import MetadataOverview from './MetadataOverview';
import TimelineChart from './MetadataTimeline';
import MapComponent from './MetadataMap';
import { getDataFormat } from './util.js';
import './MetadataTab.css';

function MetadataTab() {
    const { metadataOptions } = useContext(DBContext);
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedMetadata, setSelectedMetadata] = useState(null);

    const isLoading = !metadataOptions || metadataOptions.length === 0; // Check if metadataOptions are not loaded yet

    return (
        <div className="metadata-tab">
            {isLoading ? (
                <div className="loading-container">Loading...</div>
            ) : (
                <>
                    <MetadataOverview
                        metadataOptions={metadataOptions}
                        searchTerm={searchTerm}
                        setSearchTerm={setSearchTerm}
                        selectedMetadata={selectedMetadata}
                        setSelectedMetadata={setSelectedMetadata}
                    />
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
                    <TimelineChart metadataOptions={metadataOptions} selectedMetadata={selectedMetadata} />
                    <MapComponent metadataOptions={metadataOptions} selectedMetadata={selectedMetadata} />
                </>
            )}
        </div>
    );
}

export default MetadataTab;
