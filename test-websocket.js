// Test script to verify WebSocket real-time data
const axios = require('axios');

const BASE_URL = 'http://localhost:3000';
const TEST_SYMBOL = 'BTC_USDT';

async function testRealtimeEndpoints() {
  console.log('🧪 Testing WebSocket Real-time Endpoints...\n');
  
  try {
    // Test 1: Get regular ticker (API)
    console.log('1️⃣ Testing regular ticker endpoint...');
    const regularTicker = await axios.get(`${BASE_URL}/trading/ticker?symbol=${TEST_SYMBOL}`);
    console.log('✅ Regular ticker:', regularTicker.data.data.tickers[0].last_price);
    
    // Test 2: Get real-time ticker (WebSocket)
    console.log('\n2️⃣ Testing real-time ticker endpoint...');
    console.log('⚠️  Note: This requires authentication. You need to provide a valid JWT token.');
    console.log('   Use: GET /trading/realtime-ticker?symbol=BTC_USDT');
    console.log('   Headers: Authorization: Bearer <your-jwt-token>');
    
    // Test 3: Get real-time price (WebSocket)
    console.log('\n3️⃣ Testing real-time price endpoint...');
    console.log('⚠️  Note: This requires authentication. You need to provide a valid JWT token.');
    console.log('   Use: GET /trading/realtime-price?symbol=BTC_USDT');
    console.log('   Headers: Authorization: Bearer <your-jwt-token>');
    
    console.log('\n📋 To test with authentication:');
    console.log('1. Get a JWT token from your login endpoint');
    console.log('2. Use it in the Authorization header');
    console.log('3. Call the real-time endpoints');
    console.log('4. Check the response for "source": "websocket" or "api_fallback"');
    
  } catch (error) {
    console.error('❌ Error testing endpoints:', error.message);
  }
}

// Run the test
testRealtimeEndpoints();
