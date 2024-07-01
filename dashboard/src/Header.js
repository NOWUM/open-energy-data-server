import React, { useContext } from 'react';
import { DBContext } from './DBContext';
import SwaggerUI from 'swagger-ui-react';
import 'swagger-ui-react/swagger-ui.css';
import './RestTab.css';
function Header({ activeTab, setActiveTab }) {
    const { swaggerSpec, swaggerOptions, selectedProfile, setSelectedProfile } = useContext(DBContext);

    const isLoading = !swaggerSpec || !swaggerOptions.length;

    const handleButtonClick = (tab) => {
        setActiveTab(tab);
    }

    return (
        <>
            <div className="App-header">
                <header className="top-header">
                    <h1>OEDS Explorer</h1>
                    <div className='button-container'>
                        <button className="button" onClick={() => handleButtonClick('rest')} >REST</button>
                        <button className="button" onClick={() => handleButtonClick('metadata')} >Metadata</button>
                    </div>

                </header>
                {activeTab === 'rest' &&
                    <div className="select-container" >
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
                    </div>}
            </div>
        </>
    );
}

export default Header;
