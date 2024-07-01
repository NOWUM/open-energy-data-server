import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, Brush, ResponsiveContainer } from 'recharts';
import './MetadataTab.css';

function TimelineChart({ metadataOptions, selectedMetadata }) {
    const [chartData, setChartData] = useState([]);

    useEffect(() => {
        let activeMetadata = metadataOptions;
        if (selectedMetadata && (selectedMetadata.temporal_start || selectedMetadata.temporal_end)) {
            activeMetadata = [selectedMetadata];	
        }

        const datefulOptions = activeMetadata.filter(metadata => metadata.temporal_start && metadata.temporal_end);

        const tempDates = datefulOptions.map(metadata => ({
            schema_name: metadata.schema_name,
            start: new Date(metadata.temporal_start),
            end: new Date(metadata.temporal_end)
        })).filter(dates => !isNaN(dates.start.valueOf()) && !isNaN(dates.end.valueOf()));

        if (tempDates.length === 0) return;

        const fullTempDates = metadataOptions.map(metadata => ({
            start: new Date(metadata.temporal_start),
            end: new Date(metadata.temporal_end)
        })).filter(dates => !isNaN(dates.start.valueOf()) && !isNaN(dates.end.valueOf()));

        const minDate = new Date(Math.min(...fullTempDates.map(dates => dates.start)));
        const maxDate = new Date(Math.max(...fullTempDates.map(dates => dates.end)));

        let data = [];
        for (let m = new Date(minDate); m <= maxDate; m.setMonth(m.getMonth() + 1)) {
            let count = 0;
            let activeOptions = [];
            tempDates.forEach(({ schema_name, start, end }) => {
                if (start <= m && end >= m) {
                    count++;
                    activeOptions.push(schema_name);
                }
            });
            const yearMonth = m.toISOString().split('T')[0].slice(0, 7);
            data.push({ name: yearMonth, value: count, activeOptions });
        }
        setChartData(data);
    }, [metadataOptions, selectedMetadata]);

    const customTooltip = ({ active, payload, label }) => {
        if (active && payload && payload.length) {
            return (
                <div className="custom-tooltip">
                    <p className="label">{`${label} : ${payload[0].value}`}</p>
                    <p>Active schemas:</p>
                    <ul>
                        {payload[0].payload.activeOptions.map((item, index) => (
                            <li key={index}>{item}</li>
                        ))}
                    </ul>
                </div>
            );
        }
        return null;
    };

    return (
        <div >
            <div style={{  height: '300px' }}>
                <ResponsiveContainer>
                    <LineChart data={chartData}
                        	 margin={{ top: 5, right: 64, left: 5, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="name" />
                        <YAxis />
                        <Tooltip content={customTooltip} />
                        <Legend />
                        <Line type="monotone" dataKey="value" stroke="#8884d8" dot={false} />
                     
                    <Brush dataKey='name' padding={{ top: 5, bottom: 0 , left: 0, right: 64}} height={30} stroke="#8884d8"    />
                    </LineChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
}

export default TimelineChart;
