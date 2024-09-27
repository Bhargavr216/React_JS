import React, { useState } from 'react';
import './App.css';
import Fish from './fish';

function App() {
  // Array of cat information
  const [cats, setCats] = useState([
    {
      name: 'Rio',
      age: 1,
      breed: 'Persian',
      lastFed: '08:00 PM',
    },
    {
      name: 'Milo',
      age: 2,
      breed: 'Siamese',
      lastFed: '09:00 PM',
    },
    {
      name: 'Luna',
      age: 3,
      breed: 'Bengal',
      lastFed: '10:00 AM',
    },
    {
      name: 'kalyan',
      age: 3,
      breed: 'indai',
      lastFed: '10:00 AM',
    },
  ]);

  // Function to update the feeding time of all cats
  const feedCats = () => {
    const now = new Date().toLocaleTimeString();
    setCats(cats.map(cat => ({ ...cat, lastFed: now })));
  };

  return (
    <div className="App">
      <h1>🐱 Cat Care App</h1>
      
      {/* Cat Info Section */}
      {cats.map((cat, index) => (
        <div key={index} className="cat-info">
          <h2>Meet {cat.name}!</h2>
          <p><strong>Age:</strong> {cat.age} years</p>
          <p><strong>Breed:</strong> {cat.breed}</p>
          <p><strong>Last Fed:</strong> {cat.lastFed}</p>
        </div>
      ))}

      {/* Feed Cats Button */}
      <button onClick={feedCats}>Feed All Cats</button>      
      <Fish />
    </div>
  );
}

export default App;
