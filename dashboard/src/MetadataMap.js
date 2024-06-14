import React, { useState } from 'react';
import { MapContainer, TileLayer, GeoJSON, useMapEvents, Marker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import * as turf from '@turf/turf';
import L from 'leaflet';

function MapComponent({ metadataOptions }) {
    const position = [50.775132, 6.083861];
    const [clickedLocation, setClickedLocation] = useState(null);
    const [overlappingSources, setOverlappingSources] = useState([]);

    const getColor = (overlapCount) => {
        const baseOpacity = 0.2; 
        return `rgba(255, 0, 0, ${Math.min(1, baseOpacity + overlapCount * 0.2)})`;
    };

    const MapEvents = ({ metadataOptions }) => {
        useMapEvents({
            click: (e) => {
                const clickedPoint = turf.point([e.latlng.lng, e.latlng.lat]);
                const overlaps = metadataOptions.filter((option) => {
                    if (option.concave_hull_geometry) {
                        const polygon = turf.polygon(option.concave_hull_geometry.coordinates);
                        return turf.booleanPointInPolygon(clickedPoint, polygon);
                    }
                    return false;
                });

                setClickedLocation(e.latlng);
                setOverlappingSources(overlaps);
            }
        });
        return null;
    };

    return (
        <div className="section">
            <h3>Availability Map</h3>
            <div className="chart-container">
                <MapContainer center={position} zoom={4} style={{ height: '600px', width: '60%' }}>
                    <TileLayer
                        url="https://map.nowum.fh-aachen.de/cartodb/light_all/{z}/{x}/{y}.png"
                        attribution='&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
                    />
                    <MapEvents metadataOptions={metadataOptions} />
                    {metadataOptions.map((option, index) => (
                        <GeoJSON
                            key={index}
                            data={option.concave_hull_geometry}
                            style={{
                                color: getColor(option.overlapCount || 0),
                                weight: 0,
                                fillColor: getColor(option.overlapCount || 0),
                                fillOpacity: Math.min(1, 0.2 + (option.overlapCount || 0) * 0.2)
                            }}
                        />
                    ))}
                    {clickedLocation && (
                        <Marker position={clickedLocation}>
                            <Popup>A click occurred here!</Popup>
                        </Marker>
                    )}
                </MapContainer>
            </div>
            {overlappingSources.length > 0 && (
                <div>
                    <h4>Overlapping Sources at Click:</h4>
                    <ul>
                        {overlappingSources.map((source, index) => (
                            <li key={index}>{source.schema_name || "Unnamed Source"}</li>
                        ))}
                    </ul>
                </div>
            )}
        </div>
    );
}

export default MapComponent;
