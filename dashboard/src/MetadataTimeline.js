import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, Brush } from 'recharts';

function TimelineChart({ metadataOptions }) {
    const [chartData, setChartData] = useState([]);

    useEffect(() => {
        const datefulOptions = metadataOptions.filter(metadata => metadata.temporal_start && metadata.temporal_end);
        const tempDates = datefulOptions.map(metadata => ({
            schema_name: metadata.schema_name,
            start: new Date(metadata.temporal_start),
            end: new Date(metadata.temporal_end)
        })).filter(dates => !isNaN(dates.start.valueOf()) && !isNaN(dates.end.valueOf()));
    
        if (tempDates.length === 0) return;
    
        const minDate = new Date(Math.min(...tempDates.map(dates => dates.start)));
        const maxDate = new Date(Math.max(...tempDates.map(dates => dates.end)));
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
    }, [metadataOptions]);


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
        <div className="section">
            <h3>Availability Timeline</h3>
            <div className="chart-container">
                <LineChart width={800} height={300} data={chartData}
                    margin={{ top: 5, right: 100, left: 50, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
                    <YAxis />
                    <Tooltip content={customTooltip} />
                    <Legend />
                    <Line type="monotone" dataKey="value" stroke="#8884d8"  dot={false} />
                    <Brush dataKey='name' height={30} stroke="#8884d8" />
                </LineChart>
            </div>
        </div>
    );
}

export default TimelineChart;
