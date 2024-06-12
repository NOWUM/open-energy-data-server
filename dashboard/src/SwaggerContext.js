import React, { createContext, useState, useEffect } from 'react';

export const SwaggerContext = createContext();

export const SwaggerProvider = ({ children }) => {
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
        <SwaggerContext.Provider value={{ swaggerSpec, swaggerOptions, selectedProfile, setSelectedProfile }}>
            {children}
        </SwaggerContext.Provider>
    );
};
