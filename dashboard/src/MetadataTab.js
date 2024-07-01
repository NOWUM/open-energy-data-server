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
                    <div className='table-map'>
                        <div className="section detail-view">
                            <table className="metadata-table">
                                <tbody>
                                    <tr><td>Name</td><td>{selectedMetadata ? selectedMetadata.schema_name : "Nothing selected"}</td></tr>
                                    <tr><td>Crawl Date</td><td>{selectedMetadata ? selectedMetadata.crawl_date : 'N/A'}</td></tr>
                                    <tr><td>Data Date</td><td>{selectedMetadata ? selectedMetadata.data_date : 'N/A'}</td></tr>
                                    <tr><td>Data Source</td><td>{selectedMetadata ? selectedMetadata.data_source : 'N/A'}</td></tr>
                                    <tr><td>Licence</td><td>{selectedMetadata ? selectedMetadata.licence : 'N/A'}</td></tr>
                                    <tr><td>Description</td><td>{selectedMetadata ? selectedMetadata.description : 'N/A'}</td></tr>
                                    <tr><td>Contact</td><td>{selectedMetadata ? selectedMetadata.contact : 'N/A'}</td></tr>
                                    <tr><td>Tables</td><td>{selectedMetadata ? selectedMetadata.tables : 'N/A'}</td></tr>
                                    <tr><td>Size</td><td>{selectedMetadata && selectedMetadata.size ? `${selectedMetadata.size} bytes` : 'N/A'}</td></tr>
                                    <tr><td>Type</td><td>{selectedMetadata ? getDataFormat(selectedMetadata) : 'N/A'}</td></tr>
                                </tbody>
                            </table>

                        </div>

                            <MapComponent metadataOptions={metadataOptions} selectedMetadata={selectedMetadata} />
                    </div>

                    <TimelineChart metadataOptions={metadataOptions} selectedMetadata={selectedMetadata} />
                </>
            )}
        </div>
    );
}

export default MetadataTab;
