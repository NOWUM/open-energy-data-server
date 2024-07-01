import React, { useContext, useState } from 'react';
import { DBContext } from './DBContext';
import MetadataOverview from './MetadataOverview';
import TimelineChart from './MetadataTimeline';
import MapComponent from './MetadataMap';
import { getDataFormat, getFormattedSize } from './util.js';
import './MetadataTab.css';

function MetadataTab() {
    const { metadataOptions } = useContext(DBContext);
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedMetadata, setSelectedMetadata] = useState(null);
    const [mapZoomTrigger, setMapZoomTrigger] = useState(null);

    const isLoading = !metadataOptions || metadataOptions.length === 0; // Check if metadataOptions are not loaded yet

    const handleZoomClick = () => {
        if (selectedMetadata && selectedMetadata.concave_hull_geometry) {
            setMapZoomTrigger(selectedMetadata.concave_hull_geometry);
        }
    };

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
                                    <tr><td>Data Source</td>{selectedMetadata ? <td><a href={selectedMetadata.data_source}> {selectedMetadata.data_source}</a></td> : <td>N/A</td>}</tr>
                                    <tr><td>Licence</td><td>{selectedMetadata ? selectedMetadata.licence : 'N/A'}</td></tr>
                                    <tr><td>Description</td><td>{selectedMetadata ? selectedMetadata.description : 'N/A'}</td></tr>
                                    <tr><td>Contact</td><td>{selectedMetadata ? selectedMetadata.contact : 'N/A'}</td></tr>
                                    <tr><td>Tables</td><td>{selectedMetadata ? selectedMetadata.tables : 'N/A'}</td></tr>
                                    <tr><td>Size</td><td>{selectedMetadata && selectedMetadata.size ? getFormattedSize(selectedMetadata.size) : 'N/A'}</td></tr>
                                    <tr><td>Type</td>{ selectedMetadata ?  (selectedMetadata.concave_hull_geometry ? <td>{getDataFormat(selectedMetadata)} <button onClick={handleZoomClick} style={{ background: 'none', border: 'none', cursor: 'pointer' }}>🔍</button> </td> :<td>{  getDataFormat(selectedMetadata) } </td> ): <td>{ 'N/A'} </td>}</tr>

                                </tbody>
                            </table>
                        </div>
                        <MapComponent 
                            metadataOptions={metadataOptions} 
                            selectedMetadata={selectedMetadata} 
                            mapZoomTrigger={mapZoomTrigger}
                        />
                    </div>
                    <TimelineChart metadataOptions={metadataOptions} selectedMetadata={selectedMetadata} />
                </>
            )}
        </div>
    );
}

export default MetadataTab;
