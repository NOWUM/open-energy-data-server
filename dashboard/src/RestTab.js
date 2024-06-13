import React, { useContext } from 'react';
import SwaggerUI from 'swagger-ui-react';
import 'swagger-ui-react/swagger-ui.css';
import { DBContext } from './DBContext';

function RestTab() {
    const { swaggerSpec, swaggerOptions, selectedProfile, setSelectedProfile } = useContext(DBContext);

    return (
        <>
            <h2>REST</h2>
            <div style={{ display: 'flex' }}>
                <select style={{ flex: '1' }} value={selectedProfile} onChange={e => setSelectedProfile(e.target.value)}>
                    {swaggerOptions.map(option => (
                        <option key={option} value={option}>{option}</option>
                    ))}
                </select>
            </div>

            {swaggerSpec && <SwaggerUI spec={swaggerSpec} />}
        </>
    );
}

export default RestTab;
