import React, { useState, useEffect } from 'react';
import './App.css';
import SwaggerUI from 'swagger-ui-react';
import 'swagger-ui-react/swagger-ui.css';

function App() {
  const [swaggerSpec, setSwaggerSpec] = useState(null);
  const [swaggerOptions, setSwaggerOptions] = useState([]);
  const [selectedProfile, setSelectedProfile] = useState('');

  useEffect(() => {
    fetch("https://monitor.nowum.fh-aachen.de/oeds/rpc/swagger_schemas")
      .then(res => res.json())
      .then(data => {
        setSwaggerOptions(data);
        if (data.length > 0) {
          setSelectedProfile(data[0]); 
        }
      })
      .catch(error => console.error('Error fetching swagger schemas:', error));
  }, []);

  useEffect(() => {
    if (!selectedProfile) return; 

    const url = new URL("https://monitor.nowum.fh-aachen.de/oeds/");
    fetch(url, {
      headers: new Headers({
        'Accept-Profile': selectedProfile
      })
    }).then(response => response.text())
      .then(response => {
        setSwaggerSpec(JSON.parse(response));
      })
      .catch(error => console.error('Error fetching swagger spec:', error));
  }, [selectedProfile]);

  return (
    <div className="App">
      <header className="App-header">
        <h1>API Documentation</h1>
        <select value={selectedProfile} onChange={e => setSelectedProfile(e.target.value)}>
          {swaggerOptions.map(option => (
            <option key={option} value={option}>{option}</option>
          ))}
        </select>
        {swaggerSpec && <SwaggerUI spec={swaggerSpec} />}
      </header>
    </div>
  );
}

export default App;
