import React, { useState, useMemo } from 'react';
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const CropYieldScatterplot = () => {
  // Sample data based on your CSV - production values and price indices
  const rawData = [
    { year: 2024, priceIndex: 95.4, production: null },
    { year: 2023, priceIndex: 103, production: 233469802000 },
    { year: 2022, priceIndex: 123.4, production: 263014769000 },
    { year: 2021, priceIndex: 114.4, production: 257234506000 },
    { year: 2020, priceIndex: 100.5, production: 206442858000 },
    { year: 2019, priceIndex: 84.8, production: 174049845000 },
    { year: 2018, priceIndex: 85.8, production: 185332964000 },
    { year: 2017, priceIndex: 86.2, production: 187871601000 },
    { year: 2016, priceIndex: 85.5, production: 185945409000 },
    { year: 2015, priceIndex: 87, production: 179295311000 },
    { year: 2014, priceIndex: 92.3, production: 195281436000 },
    { year: 2013, priceIndex: 105.7, production: 210553592000 },
    { year: 2012, priceIndex: 107, production: 223867206000 },
    { year: 2011, priceIndex: 100, production: 211359330000 },
    { year: 2010, priceIndex: 87, production: 191098825000 },
    { year: 2009, priceIndex: 85.7, production: 158641034000 },
    { year: 2008, priceIndex: 95.9, production: 168375568000 },
    { year: 2007, priceIndex: 81, production: 166105717000 },
    { year: 2006, priceIndex: 68, production: null },
    { year: 2005, priceIndex: 62.6, production: null },
    { year: 2004, priceIndex: 65.5, production: null },
    { year: 2003, priceIndex: 62.8, production: null },
    { year: 2002, priceIndex: 59.7, production: null },
    { year: 2001, priceIndex: 56.4, production: null },
    { year: 2000, priceIndex: 54.4, production: null },
    { year: 1999, priceIndex: 54.9, production: null },
    { year: 1998, priceIndex: 60.6, production: null },
    { year: 1997, priceIndex: 65.5, production: null },
    { year: 1996, priceIndex: 71.9, production: null },
    { year: 1995, priceIndex: 63.7, production: null },
    { year: 1994, priceIndex: 59.9, production: null },
    { year: 1993, priceIndex: 58.2, production: null },
    { year: 1992, priceIndex: 57.3, production: null },
    { year: 1991, priceIndex: 57.1, production: null },
    { year: 1990, priceIndex: 58.5, production: null },
    { year: 1980, priceIndex: 107, production: null },
    { year: 1970, priceIndex: 225, production: null },
    { year: 1960, priceIndex: 223, production: null },
    { year: 1950, priceIndex: 233, production: null },
    { year: 1940, priceIndex: 91, production: null },
    { year: 1930, priceIndex: 115, production: null },
    { year: 1920, priceIndex: 235, production: null },
    { year: 1910, priceIndex: 106, production: null }
  ];

  const [selectedMetric, setSelectedMetric] = useState('both');
  const [showTrendLine, setShowTrendLine] = useState(true);

  // Process data for visualization
  const chartData = useMemo(() => {
    const data = [];
    
    // Add price index data points
    if (selectedMetric === 'priceIndex' || selectedMetric === 'both') {
      rawData.forEach(item => {
        if (item.priceIndex !== null) {
          data.push({
            year: item.year,
            value: item.priceIndex,
            type: 'Price Index (2011=100)',
            displayValue: item.priceIndex.toFixed(1)
          });
        }
      });
    }
    
    // Add production data points (scaled for visibility)
    if (selectedMetric === 'production' || selectedMetric === 'both') {
      rawData.forEach(item => {
        if (item.production !== null) {
          data.push({
            year: item.year,
            value: item.production / 1000000000, // Convert to billions for better scale
            type: 'Production Value (Billions $)',
            displayValue: `$${(item.production / 1000000000).toFixed(1)}B`
          });
        }
      });
    }
    
    return data;
  }, [selectedMetric]);

  // Custom tooltip
  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-3 border border-gray-300 rounded shadow-lg">
          <p className="font-semibold">{`Year: ${label}`}</p>
          <p className="text-blue-600">
            {`${data.type}: ${data.displayValue}`}
          </p>
          {data.type === 'Price Index (2011=100)' && (
            <p className="text-xs text-gray-500 mt-1">
              Relative to 2011 baseline (100)
            </p>
          )}
        </div>
      );
    }
    return null;
  };

  // Calculate trend statistics
  const trendStats = useMemo(() => {
    const priceData = rawData.filter(d => d.priceIndex !== null);
    const productionData = rawData.filter(d => d.production !== null);
    
    const avgPriceIndex = priceData.reduce((sum, d) => sum + d.priceIndex, 0) / priceData.length;
    const avgProduction = productionData.reduce((sum, d) => sum + d.production, 0) / productionData.length;
    
    return {
      priceIndexRange: `${Math.min(...priceData.map(d => d.priceIndex)).toFixed(1)} - ${Math.max(...priceData.map(d => d.priceIndex)).toFixed(1)}`,
      avgPriceIndex: avgPriceIndex.toFixed(1),
      avgProduction: `$${(avgProduction / 1000000000).toFixed(1)}B`,
      dataYears: `${Math.min(...rawData.map(d => d.year))} - ${Math.max(...rawData.map(d => d.year))}`
    };
  }, []);

  return (
    <div className="w-full max-w-6xl mx-auto p-6 bg-gradient-to-br from-green-50 to-blue-50 rounded-lg">
      <div className="mb-6">
        <h1 className="text-3xl font-bold text-gray-800 mb-2">
          🌾 U.S. Crop Totals: Historical Trends Analysis
        </h1>
        <p className="text-gray-600 mb-4">
          Interactive visualization of crop price indices and production values from 1910-2024
        </p>
        
        {/* Controls */}
        <div className="flex flex-wrap gap-4 mb-4">
          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-700">Show:</label>
            <select 
              value={selectedMetric} 
              onChange={(e) => setSelectedMetric(e.target.value)}
              className="px-3 py-1 border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="both">Both Metrics</option>
              <option value="priceIndex">Price Index Only</option>
              <option value="production">Production Value Only</option>
            </select>
          </div>
        </div>
        
        {/* Statistics */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6 text-sm">
          <div className="bg-white p-3 rounded border">
            <div className="font-semibold text-gray-700">Data Period</div>
            <div className="text-blue-600">{trendStats.dataYears}</div>
          </div>
          <div className="bg-white p-3 rounded border">
            <div className="font-semibold text-gray-700">Price Index Range</div>
            <div className="text-green-600">{trendStats.priceIndexRange}</div>
          </div>
          <div className="bg-white p-3 rounded border">
            <div className="font-semibold text-gray-700">Avg Price Index</div>
            <div className="text-purple-600">{trendStats.avgPriceIndex}</div>
          </div>
          <div className="bg-white p-3 rounded border">
            <div className="font-semibold text-gray-700">Avg Production</div>
            <div className="text-orange-600">{trendStats.avgProduction}</div>
          </div>
        </div>
      </div>

      {/* Chart */}
      <div className="bg-white p-4 rounded-lg shadow-lg">
        <ResponsiveContainer width="100%" height={500}>
          <ScatterChart margin={{ top: 20, right: 30, bottom: 60, left: 60 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
            <XAxis 
              type="number" 
              dataKey="year" 
              domain={['dataMin - 5', 'dataMax + 5']}
              tickFormatter={(value) => value}
              label={{ value: 'Year', position: 'insideBottom', offset: -10 }}
            />
            <YAxis 
              label={{ value: 'Value', angle: -90, position: 'insideLeft' }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend />
            
            {(selectedMetric === 'priceIndex' || selectedMetric === 'both') && (
              <Scatter 
                name="Price Index (2011=100)" 
                data={chartData.filter(d => d.type === 'Price Index (2011=100)')} 
                fill="#3B82F6"
                stroke="#1E40AF"
                strokeWidth={1}
              />
            )}
            
            {(selectedMetric === 'production' || selectedMetric === 'both') && (
              <Scatter 
                name="Production Value (Billions $)" 
                data={chartData.filter(d => d.type === 'Production Value (Billions $)')} 
                fill="#10B981"
                stroke="#047857"
                strokeWidth={1}
              />
            )}
          </ScatterChart>
        </ResponsiveContainer>
      </div>

      {/* Key Insights */}
      <div className="mt-6 bg-white p-4 rounded-lg shadow">
        <h3 className="text-lg font-semibold text-gray-800 mb-3">📊 Key Insights</h3>
        <div className="grid md:grid-cols-2 gap-4 text-sm">
          <div>
            <h4 className="font-semibold text-blue-600 mb-2">Price Index Trends</h4>
            <ul className="space-y-1 text-gray-700">
              <li>• Historical volatility with major spikes around 1920 and 1970</li>
              <li>• Recent peak in 2022 (123.4) during supply chain disruptions</li>
              <li>• Long-term average around {trendStats.avgPriceIndex}</li>
            </ul>
          </div>
          <div>
            <h4 className="font-semibold text-green-600 mb-2">Production Value Trends</h4>
            <ul className="space-y-1 text-gray-700">
              <li>• Steady growth from 2007-2023 (data available period)</li>
              <li>• Peak production value in 2022 ($263B)</li>
              <li>• Strong correlation with price index movements</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CropYieldScatterplot;
