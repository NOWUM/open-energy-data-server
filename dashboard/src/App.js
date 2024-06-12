import React, { useState } from 'react';
import './App.css';
import RestTab from './RestTab';
import MetadataTab from './MetadataTab';
import { SwaggerProvider } from './SwaggerContext';

function App() {
  const [activeTab, setActiveTab] = useState('rest');

  return (
    <SwaggerProvider>
      <div className="App">
        <header className="App-header">
          <h1>OEDS Explorer</h1>
          <div style={{ display: 'flex' }}>
            <button style={{ flex: 1 }} onClick={() => setActiveTab('rest')}>REST</button>
            <button style={{ flex: 1 }} onClick={() => setActiveTab('metadata')}>Metadata</button>
          </div>
        </header>
        <div className="tab-content">
          {activeTab === 'rest' && <RestTab />}
          {activeTab === 'metadata' && <MetadataTab />}
        </div>
      </div>
    </SwaggerProvider>
  );
}

export default App;
