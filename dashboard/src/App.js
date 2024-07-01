// App.js
import React, { useState, useContext } from 'react';
import { DBContext } from './DBContext';
import './App.css';
import RestTab from './RestTab';
import MetadataTab from './MetadataTab';
import { DBProvider } from './DBContext';
import { center } from '@turf/turf';
import Header from './Header';

function App() {
  const [activeTab, setActiveTab] = useState('rest');

  return (
    <DBProvider>
      <div className="App">
        <Header activeTab={activeTab} setActiveTab={setActiveTab} />
        
        <div className="tab-content">
          {activeTab === 'rest' && <RestTab />}
          {activeTab === 'metadata' && <MetadataTab />}
        </div>
      </div>
    </DBProvider>
  );
}

export default App;
