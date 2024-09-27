import React, { useState } from 'react';
import './App.css';

function Fish() {
  // fish's information
  const [fish, setCat] = useState({
    name: 'Sundeep',
    age: 1,
    breed: 'DAL',
    lastFed: '08:00 PM',
  });

  // Function to update the feeding time
  const feedCat = () => {
    const now = new Date().toLocaleTimeString();
    setCat({ ...fish, lastFed: now });
  };

  return (
    <div className="App">
      <h1>🦈 fish Care App</h1>
      
      {/* fish Info Section */}
      <div className="fish-info">
        <h2>Meet {fish.name}!</h2>
        <p><strong>Age:</strong> {fish.age} years</p>
        <p><strong>Breed:</strong> {fish.breed}</p>
        <p><strong>Last Fed:</strong> {fish.lastFed}</p>
      </div>

      {/* Feed fish Button */}
      <button onClick={feedCat}>Feed {fish.name}</button>
    </div>
  );
}

export default Fish;
