import React, { useContext } from 'react';
import SwaggerUI from 'swagger-ui-react';
import 'swagger-ui-react/swagger-ui.css';
import { DBContext } from './DBContext';
import './RestTab.css'; 
function RestTab() {
    const { swaggerSpec, swaggerOptions, selectedProfile, setSelectedProfile } = useContext(DBContext);

    const isLoading = !swaggerSpec || !swaggerOptions.length; 
    return (
        <>
            <div className="select-container" style={{ display: 'flex' }}>
                <select 
                    value={selectedProfile}
                    onChange={e => setSelectedProfile(e.target.value)}
                    disabled={!swaggerOptions.length} 
                >
                    {swaggerOptions.length > 0 ? (
                        swaggerOptions.map(option => (
                            <option key={option} value={option}>{option}</option>
                        ))
                    ) : (
                        <option>Loading options...</option> 
                    )}
                </select>
            </div>

            {isLoading && <div className="loading-container">Loading...</div>} 
            {swaggerSpec && <SwaggerUI spec={swaggerSpec} />}
        </>
    );
}

export default RestTab;
