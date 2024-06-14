import React, { createContext, useState, useEffect } from 'react';

export const DBContext = createContext();

export const DBProvider = ({ children }) => {
    const [swaggerSpec, setSwaggerSpec] = useState(null);
    const [swaggerOptions, setSwaggerOptions] = useState([]);
    const [selectedProfile, setSelectedProfile] = useState('');
    const [metadataOptions, setMetadataOptions] = useState([]);

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

    useEffect(() => {
        if (!selectedProfile) return;

        const url = new URL("https://monitor.nowum.fh-aachen.de/oeds/metadata");
        fetch(url)
        .then(res => res.json())
        .then(data => {
            console.log(data);
            setMetadataOptions(data);
        })
        .catch(error => console.error('Error fetching swagger schemas:', error));
    }, [selectedProfile]);

    return (
        <DBContext.Provider value={{ swaggerSpec, swaggerOptions, metadataOptions, selectedProfile, setSelectedProfile }}>
            {children}
        </DBContext.Provider>
    );
};
