// Test script to verify price system logging
const axios = require('axios');

const BASE_URL = 'http://localhost:3000';

async function testPriceSystem() {
  console.log('🧪 Testing Price System Logging...\n');
  
  try {
    // Test 1: Old system (should show LEGACY_SYSTEM logs)
    console.log('1️⃣ Testing OLD system (BitMart API only)...');
    const oldTicker = await axios.get(`${BASE_URL}/trading/ticker?symbol=BTC_USDT`);
    console.log('✅ Old system result:', oldTicker.data.data.tickers[0].last_price);
    
    // Test 2: New system (should show PRICE_SYSTEM and PRICE_AGGREGATOR logs)
    console.log('\n2️⃣ Testing NEW system (Multi-source aggregation)...');
    console.log('⚠️  Note: This requires authentication. Check server logs for:');
    console.log('   - [PRICE_SYSTEM] logs');
    console.log('   - [PRICE_AGGREGATOR] logs');
    console.log('   - [PRICE_SOURCE] logs');
    console.log('   - [LEGACY_SYSTEM] logs (if fallback occurs)');
    
    console.log('\n📋 What to look for in server logs:');
    console.log('✅ [PRICE_SYSTEM] 🚀 ACTIVATING MULTI-SOURCE PRICE AGGREGATION');
    console.log('✅ [PRICE_AGGREGATOR] 🎯 ENTRY: Starting multi-source price fetch');
    console.log('✅ [PRICE_SOURCE] 🟡 FETCHING from Binance');
    console.log('✅ [PRICE_SOURCE] 🟢 FETCHING from CoinGecko');
    console.log('✅ [PRICE_SOURCE] 🔵 FETCHING from CryptoCompare');
    console.log('✅ [PRICE_SOURCE] 🟠 FETCHING from BitMart');
    console.log('✅ [PRICE_AGGREGATOR] ✅ SUCCESS: BTC_USDT = $XXXXX (4 sources)');
    
    console.log('\n🔍 If you see [LEGACY_SYSTEM] logs, the new system is NOT being used!');
    
  } catch (error) {
    console.error('❌ Error testing price system:', error.message);
  }
}

// Run the test
testPriceSystem();
