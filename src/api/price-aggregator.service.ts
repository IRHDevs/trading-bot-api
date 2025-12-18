// src/api/price-aggregator.service.ts

import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import { getUserLogger } from './logger';

interface PriceSource {
  name: string;
  weight: number; // Higher weight = more trusted
  lastUpdate: number;
  isActive: boolean;
}

interface PriceData {
  price: number;
  source: string;
  timestamp: number;
  volume24h?: number;
  change24h?: number;
}

@Injectable()
export class PriceAggregatorService {
  private readonly logger = new Logger(PriceAggregatorService.name);
  private priceCache = new Map<string, PriceData>();
  private readonly CACHE_DURATION = 5000; // 5 seconds

  // Price sources with weights (higher = more trusted)
  private readonly PRICE_SOURCES = [
    { name: 'binance', weight: 10, isActive: true },
    { name: 'coingecko', weight: 8, isActive: true },
    { name: 'cryptocompare', weight: 7, isActive: true },
    { name: 'bitmart', weight: 6, isActive: true },
    { name: 'coinmarketcap', weight: 9, isActive: true },
  ];

  /**
   * Get aggregated price from multiple sources
   */
  async getAggregatedPrice(symbol: string, userId?: number): Promise<PriceData> {
    const logger = userId ? getUserLogger(userId) : this.logger;
    
    try {
      // INTERNAL LOG: Price aggregation system entry
      console.log(`[PRICE_AGGREGATOR] 🎯 ENTRY: Starting multi-source price fetch for ${symbol} (User: ${userId || 'system'})`);
      
      // Check cache first
      const cached = this.priceCache.get(symbol);
      if (cached && Date.now() - cached.timestamp < this.CACHE_DURATION) {
        console.log(`[PRICE_AGGREGATOR] ⚡ CACHE HIT: ${symbol} = $${cached.price} (${cached.source})`);
        logger.debug(`Using cached price for ${symbol}: ${cached.price} from ${cached.source}`);
        return cached;
      }

      console.log(`[PRICE_AGGREGATOR] 🔄 CACHE MISS: Fetching fresh data for ${symbol}`);
      logger.log(`Fetching aggregated price for ${symbol} from multiple sources...`, 'info');

      // Fetch from all active sources in parallel
      const pricePromises = this.PRICE_SOURCES
        .filter(source => source.isActive)
        .map(source => this.fetchFromSource(symbol, source.name, source.weight));

      const results = await Promise.allSettled(pricePromises);
      
      // Filter successful results
      const validPrices = results
        .filter((result): result is PromiseFulfilledResult<PriceData> => result.status === 'fulfilled')
        .map(result => result.value)
        .filter(price => price && price.price > 0);

      if (validPrices.length === 0) {
        throw new Error(`No valid prices found for ${symbol}`);
      }

      // Calculate weighted average
      const aggregatedPrice = this.calculateWeightedAverage(validPrices);
      
      // INTERNAL LOG: Aggregation success
      console.log(`[PRICE_AGGREGATOR] ✅ SUCCESS: ${symbol} = $${aggregatedPrice.price} (${validPrices.length} sources: ${validPrices.map(p => p.source).join(', ')})`);
      
      // Cache the result
      this.priceCache.set(symbol, aggregatedPrice);
      
      logger.log(`Aggregated price for ${symbol}: ${aggregatedPrice.price} (from ${validPrices.length} sources)`, 'info');
      
      return aggregatedPrice;
    } catch (error) {
      logger.error(`Error getting aggregated price for ${symbol}: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Fetch price from a specific source
   */
  private async fetchFromSource(symbol: string, source: string, weight: number): Promise<PriceData> {
    try {
      switch (source) {
        case 'binance':
          return await this.fetchFromBinance(symbol);
        case 'coingecko':
          return await this.fetchFromCoinGecko(symbol);
        case 'cryptocompare':
          return await this.fetchFromCryptoCompare(symbol);
        case 'bitmart':
          return await this.fetchFromBitMart(symbol);
        case 'coinmarketcap':
          return await this.fetchFromCoinMarketCap(symbol);
        default:
          throw new Error(`Unknown source: ${source}`);
      }
    } catch (error) {
      this.logger.warn(`Failed to fetch from ${source} for ${symbol}: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Fetch from Binance (highest volume exchange)
   */
  private async fetchFromBinance(symbol: string): Promise<PriceData> {
    console.log(`[PRICE_SOURCE] 🟡 FETCHING from Binance: ${symbol}`);
    const binanceSymbol = this.convertToBinanceSymbol(symbol);
    const response = await axios.get(`https://api.binance.com/api/v3/ticker/price?symbol=${binanceSymbol}`, {
      timeout: 3000
    });
    
    const price = parseFloat(response.data.price);
    console.log(`[PRICE_SOURCE] ✅ Binance: ${symbol} = $${price}`);
    
    return {
      price: price,
      source: 'binance',
      timestamp: Date.now()
    };
  }

  /**
   * Fetch from CoinGecko (reliable free API)
   */
  private async fetchFromCoinGecko(symbol: string): Promise<PriceData> {
    console.log(`[PRICE_SOURCE] 🟢 FETCHING from CoinGecko: ${symbol}`);
    const coinId = this.convertToCoinGeckoId(symbol);
    const response = await axios.get(`https://api.coingecko.com/api/v3/simple/price?ids=${coinId}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true`, {
      timeout: 3000
    });
    
    const data = response.data[coinId];
    const price = data.usd;
    console.log(`[PRICE_SOURCE] ✅ CoinGecko: ${symbol} = $${price}`);
    
    return {
      price: price,
      source: 'coingecko',
      timestamp: Date.now(),
      change24h: data.usd_24h_change,
      volume24h: data.usd_24h_vol
    };
  }

  /**
   * Fetch from CryptoCompare (comprehensive data)
   */
  private async fetchFromCryptoCompare(symbol: string): Promise<PriceData> {
    console.log(`[PRICE_SOURCE] 🔵 FETCHING from CryptoCompare: ${symbol}`);
    const fsym = symbol.split('_')[0];
    const response = await axios.get(`https://min-api.cryptocompare.com/data/price?fsym=${fsym}&tsyms=USD`, {
      timeout: 3000
    });
    
    const price = response.data.USD;
    console.log(`[PRICE_SOURCE] ✅ CryptoCompare: ${symbol} = $${price}`);
    
    return {
      price: price,
      source: 'cryptocompare',
      timestamp: Date.now()
    };
  }

  /**
   * Fetch from BitMart (your current source)
   */
  private async fetchFromBitMart(symbol: string): Promise<PriceData> {
    console.log(`[PRICE_SOURCE] 🟠 FETCHING from BitMart: ${symbol}`);
    const response = await axios.get(`https://api-cloud.bitmart.com/spot/v1/ticker?symbol=${symbol}`, {
      timeout: 3000
    });
    
    const ticker = response.data.data.tickers[0];
    const price = parseFloat(ticker.last_price);
    console.log(`[PRICE_SOURCE] ✅ BitMart: ${symbol} = $${price}`);
    
    return {
      price: price,
      source: 'bitmart',
      timestamp: Date.now(),
      change24h: parseFloat(ticker.change_24h),
      volume24h: parseFloat(ticker.volume_24h)
    };
  }

  /**
   * Fetch from CoinMarketCap (professional data)
   */
  private async fetchFromCoinMarketCap(symbol: string): Promise<PriceData> {
    // Note: This requires an API key for production use
    // For now, we'll use the free tier or skip if no key
    const apiKey = process.env.COINMARKETCAP_API_KEY;
    if (!apiKey) {
      throw new Error('CoinMarketCap API key not configured');
    }

    const response = await axios.get(`https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=${symbol.split('_')[0]}`, {
      headers: {
        'X-CMC_PRO_API_KEY': apiKey
      },
      timeout: 3000
    });
    
    const data = response.data.data[symbol.split('_')[0]];
    return {
      price: data.quote.USD.price,
      source: 'coinmarketcap',
      timestamp: Date.now(),
      change24h: data.quote.USD.percent_change_24h,
      volume24h: data.quote.USD.volume_24h
    };
  }

  /**
   * Calculate weighted average of prices
   */
  private calculateWeightedAverage(prices: PriceData[]): PriceData {
    if (prices.length === 1) {
      return prices[0];
    }

    // Get weights for each source
    const weights = prices.map(price => {
      const source = this.PRICE_SOURCES.find(s => s.name === price.source);
      return source ? source.weight : 1;
    });

    // Calculate weighted average
    const totalWeight = weights.reduce((sum, weight) => sum + weight, 0);
    const weightedSum = prices.reduce((sum, price, index) => {
      return sum + (price.price * weights[index]);
    }, 0);

    const averagePrice = weightedSum / totalWeight;

    // Find the most reliable source for metadata
    const mostReliableSource = prices.reduce((best, current) => {
      const currentWeight = this.PRICE_SOURCES.find(s => s.name === current.source)?.weight || 1;
      const bestWeight = this.PRICE_SOURCES.find(s => s.name === best.source)?.weight || 1;
      return currentWeight > bestWeight ? current : best;
    });

    return {
      price: averagePrice,
      source: `aggregated(${prices.length} sources)`,
      timestamp: Date.now(),
      change24h: mostReliableSource.change24h,
      volume24h: mostReliableSource.volume24h
    };
  }

  /**
   * Convert symbol to Binance format
   */
  private convertToBinanceSymbol(symbol: string): string {
    // Convert BTC_USDT to BTCUSDT
    return symbol.replace('_', '');
  }

  /**
   * Convert symbol to CoinGecko ID
   */
  private convertToCoinGeckoId(symbol: string): string {
    const base = symbol.split('_')[0].toLowerCase();
    
    // Common mappings
    const mappings: Record<string, string> = {
      'btc': 'bitcoin',
      'eth': 'ethereum',
      'bnb': 'binancecoin',
      'ada': 'cardano',
      'sol': 'solana',
      'dot': 'polkadot',
      'matic': 'matic-network',
      'avax': 'avalanche-2',
      'link': 'chainlink',
      'uni': 'uniswap',
      'ltc': 'litecoin',
      'bch': 'bitcoin-cash',
      'xrp': 'ripple',
      'doge': 'dogecoin',
      'shib': 'shiba-inu'
    };
    
    return mappings[base] || base;
  }

  /**
   * Get price from a specific source only
   */
  async getPriceFromSource(symbol: string, source: string, userId?: number): Promise<PriceData> {
    const logger = userId ? getUserLogger(userId) : this.logger;
    
    try {
      const sourceConfig = this.PRICE_SOURCES.find(s => s.name === source);
      if (!sourceConfig) {
        throw new Error(`Unknown source: ${source}`);
      }

      const price = await this.fetchFromSource(symbol, source, sourceConfig.weight);
      logger.log(`Price from ${source} for ${symbol}: ${price.price}`, 'info');
      
      return price;
    } catch (error) {
      logger.error(`Error fetching price from ${source} for ${symbol}: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Get all available sources
   */
  getAvailableSources(): string[] {
    return this.PRICE_SOURCES
      .filter(source => source.isActive)
      .map(source => source.name);
  }

  /**
   * Update source status
   */
  updateSourceStatus(source: string, isActive: boolean): void {
    const sourceConfig = this.PRICE_SOURCES.find(s => s.name === source);
    if (sourceConfig) {
      sourceConfig.isActive = isActive;
      this.logger.log(`Source ${source} ${isActive ? 'enabled' : 'disabled'}`);
    }
  }

  /**
   * Clear price cache
   */
  clearCache(): void {
    this.priceCache.clear();
    this.logger.log('Price cache cleared');
  }
}
