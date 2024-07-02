import React, { useState, useEffect, useRef } from 'react';
import { MapContainer, TileLayer, useMapEvents, ZoomControl } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import * as turf from '@turf/turf';
import L from 'leaflet';
import './MetadataTab.css';
import icon from 'leaflet/dist/images/marker-icon.png';
import iconShadow from 'leaflet/dist/images/marker-shadow.png';

function MapComponent({ metadataOptions, selectedMetadata }) {
    const position = [50.775132, 6.083861];
    const [activeMetadata, setActiveMetadata] = useState([]);
    const mapRef = useRef(null);
    const geoJsonLayersRef = useRef([]);

    let DefaultIcon = L.icon({
        iconUrl: icon,
        shadowUrl: iconShadow,
        iconSize: [25, 41],
        iconAnchor: [12, 41],
        popupAnchor: [1, -34],
        tooltipAnchor: [16, -28],
        shadowSize: [41, 41]
    });

    L.Marker.prototype.options.icon = DefaultIcon;

    useEffect(() => {
        setActiveMetadata(selectedMetadata && selectedMetadata.concave_hull_geometry ? [selectedMetadata] : metadataOptions);
    }, [metadataOptions, selectedMetadata]);

    useEffect(() => {
        if (mapRef.current) {
            geoJsonLayersRef.current.forEach(layer => mapRef.current.removeLayer(layer));
            geoJsonLayersRef.current = [];

            if (selectedMetadata && selectedMetadata.concave_hull_geometry) {
                const geoJsonLayer = L.geoJSON(selectedMetadata.concave_hull_geometry, {
                    style: {
                        color: 'rgb(    255, 0, 0)',
                        weight: 0,
                        fillColor: 'rgb(255, 0, 0)',
                        fillOpacity: 0.2
                    }
                }).addTo(mapRef.current);
                geoJsonLayersRef.current.push(geoJsonLayer);
            } else if (!selectedMetadata) {
                activeMetadata.forEach(option => {
                    const geoJsonLayer = L.geoJSON(option.concave_hull_geometry, {
                        style: {
                            color: 'rgb(255, 0, 0)',
                            weight: 0,
                            fillColor: 'rgb(255, 0, 0)',
                            fillOpacity: 0.2
                        }
                    }).addTo(mapRef.current);
                    geoJsonLayersRef.current.push(geoJsonLayer);
                })
            }




        }
    }, [activeMetadata, selectedMetadata]);

    const MapEvents = () => {
        useMapEvents({
            click: (e) => {
                const clickedPoint = turf.point([e.latlng.lng, e.latlng.lat]);
                const overlaps = activeMetadata.filter((option) => {
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
        <div className="chart-container">
            <MapContainer ref={mapRef} zoomControl={false} center={position} zoom={4} style={{ height: '38vh', width: '100%', minHeight: '280px' }}>
                <TileLayer
                    url="https://map.nowum.fh-aachen.de/cartodb/light_all/{z}/{x}/{y}.png"
                    attribution='&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
                />
                <ZoomControl position='bottomleft' />
                <MapEvents />

            </MapContainer>
        </div>

    );
}
export default MapComponent;
