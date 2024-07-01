import React, { useContext } from 'react';
import { DBContext } from './DBContext';
import SwaggerUI from 'swagger-ui-react';
import 'swagger-ui-react/swagger-ui.css';
import './RestTab.css';
function RestTab() {
    const { swaggerSpec, swaggerOptions, selectedProfile, setSelectedProfile } = useContext(DBContext);

    const isLoading = !swaggerSpec || !swaggerOptions.length;
    return (
        <>
            <div className="rest-tab">

                {isLoading && <div className="loading-container">Loading...</div>}
                {!isLoading && swaggerSpec && <SwaggerUI spec={swaggerSpec} />}
            </div>
        </>
    );
}

export default RestTab;
