// src/trading/trading.service.ts

import {
  Injectable,
  UnauthorizedException,
  BadRequestException,
  Logger,
} from "@nestjs/common";
import axios from "axios";
import * as crypto from "crypto";
import * as ccxt from "ccxt";
import { getUserLogger } from "./logger"; // Import the logger factory
import { getTopTrendingCoinsForTheDay } from "./gainer";
import { UserRepository } from "./user/user-repository";
import { SymbolHelper } from "./utils/symbol.helper"; // Ensure correct path
import { WebSocketService } from "./websocket/websocket.service";
import { PriceAggregatorService } from "./price-aggregator.service";

const BITMART_API_URL = "https://api-cloud.bitmart.com";
interface PurchaseInfo {
  price: number;
  timestamp: number;
  quantity: number;
  sold?: boolean;
  rebuyPercentage?: number; // <-- declare here
  profitThresholds: number[]; // Array of profit percentages to sell at
}

interface UserTradeState {
  purchasePrices: Record<
    string,
    { price: number; timestamp: number; quantity: number; sold?: boolean; rebuyPercentage?: number; profitThresholds: number[]; }
  >;
  lastRecordedPrices: Record<string, number>; // New field to store last recorded current price
  profitTarget: number;
  accumulatedProfit: number;
  startDayTimestamp: number;
  payloadLogs: Record<string, any[]>;
  monitorIntervals: Record<string, NodeJS.Timeout>;
  activeMonitoringIntervals: Record<string, NodeJS.Timeout>;
  profitCheckThreshold: number;     // For normal trading
  lossCheckThreshold: number;       // For normal trading
  afterSaleProfitThreshold: number; // For after-sale monitoring
  afterSaleLossThreshold: number;   // For after-sale monitoring
  profitThresholds: number[]; // Default thresholds for new trades
  activeTrades: string[];
  afterSaleMonitorIntervals: { [key: string]: NodeJS.Timeout };
  // Loss protection fields
  consecutiveLosses: number;        // Track consecutive losing trades
  protectiveMode: boolean;          // Whether bot is in protective mode
  lastSellResult: 'profit' | 'loss' | null; // Track last sell result
  protectiveModeUntil?: number;     // Timestamp when protective mode expires
  dailyLossLimit?: number;          // Daily loss limit (in USDT, e.g., -3% of starting equity)
  tradingPausedUntil?: number;      // Timestamp when trading is paused
  consecutiveLossesBySymbol?: Record<string, number>; // Per-symbol loss tracking
  protectiveModeBySymbol?: Record<string, boolean>;  // Per-symbol protective mode
  startingEquity?: number;          // Starting equity for daily loss calculation
}
@Injectable()
export class TradingService {
  private readonly logger = new Logger(TradingService.name);
  private payloadLogs: Record<string, any[]> = {};
  private purchasePrices: Record<
    string,
    { price: number; timestamp: number; quantity: number; sold?: boolean }
  > = {};
  private monitorIntervals: Record<string, NodeJS.Timeout> = {};
  private profitTarget: number = 0;
  private accumulatedProfit: number = 0;
  private startDayTimestamp: number = 0;
  private skyrocketProfitMode: boolean = false;
  private skyrocketProfitTarget: number = 0;
  private activeMonitoringIntervals: Record<string, NodeJS.Timeout> = {};
  private userTrades = new Map<number, UserTradeState>();
  private userTradeStates: Map<number, UserTradeState> = new Map();
  private DEFAULT_PROFIT_THRESHOLDS = [1, 3]; // Default profit percentages
  private websocketSubscriptions = new Map<string, Set<number>>(); // symbol -> Set of userIds
  
  // Monitoring intervals (in milliseconds) - optimized for speed while respecting API limits
  // BitMart public endpoints allow ~10-20 requests/second, so:
  // - 2000ms = 30 req/min = 0.5 req/sec (very safe)
  // - 1000ms = 60 req/min = 1 req/sec (safe)
  private readonly MONITORING_INTERVAL = parseInt(process.env.MONITORING_INTERVAL_MS || '2000', 10); // Default: 2 seconds
  private readonly AFTER_SALE_MONITORING_INTERVAL = parseInt(process.env.AFTER_SALE_MONITORING_INTERVAL_MS || '2000', 10); // Default: 2 seconds
  private readonly BACKUP_API_CHECK_INTERVAL = parseInt(process.env.BACKUP_API_CHECK_INTERVAL_MS || '10000', 10); // Default: 10 seconds

  constructor(
    private readonly userRepository: UserRepository,
    private readonly webSocketService: WebSocketService,
    private readonly priceAggregatorService: PriceAggregatorService
  ) {
    // Set up WebSocket event listeners
    this.setupWebSocketListeners();
  }

  // Load user API keys dynamically
  private async getUserApiKeys(userId: number): Promise<{
    bitmartApiKey: string;
    bitmartApiSecret: string;
    bitmartApiMemo: string;
    monitoringApiKey: string;
    monitoringApiSecret: string;
    monitoringApiMemo: string;
  }> {
    const user = await this.userRepository.findOne({ where: { id: userId } });
    if (!user || !user.apiKeys) {
      throw new UnauthorizedException("API keys not found for this user");
    }
    const {
      bitmartApiKey,
      bitmartApiSecret,
      bitmartApiMemo,
      monitoringApiKey,
      monitoringApiSecret,
      monitoringApiMemo,
    } = user.apiKeys;

    if (
      !bitmartApiKey ||
      !bitmartApiSecret ||
      !bitmartApiMemo ||
      !monitoringApiKey ||
      !monitoringApiSecret ||
      !monitoringApiMemo
    ) {
      throw new UnauthorizedException("Incomplete API keys for this user");
    }

    return {
      bitmartApiKey,
      bitmartApiSecret,
      bitmartApiMemo,
      monitoringApiKey,
      monitoringApiSecret,
      monitoringApiMemo,
    };
  }

  private async initializeExchanges(userId: number): Promise<{
    exchange: ccxt.bitmart;
    monitoringExchange: ccxt.bitmart;
  }> {
    const {
      bitmartApiKey,
      bitmartApiSecret,
      bitmartApiMemo,
      monitoringApiKey,
      monitoringApiSecret,
      monitoringApiMemo,
    } = await this.getUserApiKeys(userId);

    const exchange = new ccxt.bitmart({
      apiKey: bitmartApiKey,
      secret: bitmartApiSecret,
      uid: bitmartApiMemo,
      verbose: true, // Enable verbose logging for debugging
      options: {
        createMarketBuyOrderRequiresPrice: false, // Disable price requirement
      },
    });

    await exchange.loadMarkets(); // Load markets

    const monitoringExchange = new ccxt.bitmart({
      apiKey: monitoringApiKey,
      secret: monitoringApiSecret,
      uid: monitoringApiMemo,
      verbose: true, // Enable verbose logging for debugging
      options: {
        createMarketBuyOrderRequiresPrice: false, // Disable price requirement
      },
    });

    await monitoringExchange.loadMarkets(); // Load markets for monitoring exchange

    return { exchange, monitoringExchange };
  }

  private generateSignature(
    httpMethod: string,
    url: string,
    timestamp: string,
    queryString: string,
    body: any,
    secretKey: string,
    memo: string,
  ): string {
    const bodyString =
      body && Object.keys(body).length > 0 ? JSON.stringify(body) : "";
    const preHashString = `${timestamp}#${memo}#${httpMethod}#${url}${queryString ? "?" + queryString : ""}${bodyString}`;
    return crypto
      .createHmac("sha256", secretKey)
      .update(preHashString)
      .digest("hex");
  }

  private async getAuthHeaders(
    userId: number,
    endpoint: string,
    method: string,
    queryString: string,
    body: any,
  ): Promise<any> {
    const { bitmartApiKey, bitmartApiSecret, bitmartApiMemo } =
      await this.getUserApiKeys(userId);
    const timestamp = Date.now().toString();
    const urlPath = endpoint.replace(BITMART_API_URL, "");
    const signature = this.generateSignature(
      method,
      urlPath,
      timestamp,
      queryString,
      body,
      bitmartApiSecret,
      bitmartApiMemo,
    );

    return {
      "X-BM-KEY": bitmartApiKey,
      "X-BM-SIGN": signature,
      "X-BM-TIMESTAMP": timestamp,
      "X-BM-MEMO": bitmartApiMemo,
      "Content-Type": "application/json",
    };
  }
  public getUserTradeState(userId: number): UserTradeState {
    let state = this.userTrades.get(userId);
    if (!state) {
      state = this.initializeTradeState(userId);
    }
    return state; // Now state is guaranteed to be UserTradeState
  }
  private async getMonitoringAuthHeaders(
    userId: number,
    endpoint: string,
    method: string,
    queryString: string,
    body: any,
  ): Promise<any> {
    const { monitoringApiKey, monitoringApiSecret, monitoringApiMemo } =
      await this.getUserApiKeys(userId);
    const timestamp = Date.now().toString();
    const urlPath = endpoint.replace(BITMART_API_URL, "");
    const signature = this.generateSignature(
      method,
      urlPath,
      timestamp,
      queryString,
      body,
      monitoringApiSecret,
      monitoringApiMemo,
    );

    return {
      "X-BM-KEY": monitoringApiKey,
      "X-BM-SIGN": signature,
      "X-BM-TIMESTAMP": timestamp,
      "X-BM-MEMO": monitoringApiMemo,
      "Content-Type": "application/json",
    };
  }

  // Add this helper method inside your TradingService class
  private async ensureSellCompleted(
    userId: number,
    symbol: string,
    expectedSoldQuantity: number,
  ): Promise<void> {
    const logger = getUserLogger(userId);
    const state = this.getUserTradeState(userId);

    try {
      // Check if already sold
      if (state.purchasePrices[symbol]?.sold) {
        logger.info(`${symbol} already marked as sold, proceeding to after-sale monitoring`);
        const rebuyPercentage = state.purchasePrices[symbol]?.rebuyPercentage || 10;
        await this.startMonitoringAfterSale(userId, symbol, rebuyPercentage);
        return;
      }

      // Get remaining balance with retry
      const remainingQuantity = await this.getSymbolBalance(userId, symbol);
      const currentPrice = await this.fetchTickerWithRetry(symbol);
      const remainingValue = remainingQuantity * currentPrice;

      if (remainingValue < 1) {
        logger.info(`Sell confirmed for ${symbol}. Remaining value ($${remainingValue.toFixed(2)}) is below $1.`);
        
        // Update state ONCE
        if (state.purchasePrices[symbol]) {
          state.purchasePrices[symbol].sold = true;
          state.purchasePrices[symbol].quantity = 0;
        }

        // Handle profit calculation
        await this.checkAndHandleProfit(userId, symbol, expectedSoldQuantity, currentPrice);

        // Start after-sale monitoring ONCE
        const rebuyPercentage = state.purchasePrices[symbol]?.rebuyPercentage || 10;
        logger.info(`Transitioning to after-sale monitoring for ${symbol}`);
        await this.startMonitoringAfterSale(userId, symbol, rebuyPercentage);
        return;
      }

      throw new Error(`Sell not fully confirmed for ${symbol}. Remaining value: $${remainingValue.toFixed(2)}`);
    } catch (error) {
      logger.error(`Error during sell confirmation for ${symbol}: ${(error as Error).message}`);
      throw error;
    }
  }
  
  public getAccumulatedProfit(userId: number): number {
    const state = this.getUserTradeState(userId);
  
    if (!state) {
      return 0;
    }
  
    return state.accumulatedProfit;
  }
  

  /**
   * Retrieves the user's balance for a specific currency.
   * @param userId - The ID of the user.
   * @param currency - The currency symbol (default: 'USDT').
   * @returns The available balance.
   */
  public async getUserBalance(
    userId: number,
    currency: string = "USDT",
  ): Promise<number> {
    const logger = getUserLogger(userId);
    const url = `${BITMART_API_URL}/account/v1/wallet`;

    try {
      logger.info("Starting balance fetch process.");

      const headers = await this.getAuthHeaders(
        userId,
        "/account/v1/wallet",
        "GET",
        "",
        {},
      );

      logger.info("Fetching user balance from BitMart API.");

      const response = await axios.get(url, { headers });

      if (
        !response.data ||
        !response.data.data ||
        !Array.isArray(response.data.data.wallet)
      ) {
        logger.error("Unexpected API response structure");
        throw new Error("Unexpected API response structure");
      }

      const balanceEntry = response.data.data.wallet.find(
        (b: any) => b.currency.toUpperCase() === currency.toUpperCase(),
      );

      const availableBalance = balanceEntry
        ? parseFloat(balanceEntry.available)
        : 0;

      logger.info(
        `User balance retrieved successfully: ${availableBalance} ${currency}`,
      );

      return availableBalance;
    } catch (error: unknown) {
      const err = error as any;
      logger.error(
        "Error fetching user balance:",
        err.message || "Unknown error",
      );
      throw new Error("Failed to fetch user balance");
    }
  }

  /**
   * Fetches the latest ticker price for a given symbol.
   * @param symbol - The trading symbol in "BASE_QUOTE" format (e.g., "PWC_USDT").
   * @returns The last traded price.
   */
  private async fetchTicker(symbol: string): Promise<number> {
    // INTERNAL LOG: Old system usage
    console.log(`[LEGACY_SYSTEM] 📡 USING OLD BITMART API for ${symbol}`);
    
    const formattedSymbol = SymbolHelper.toCCXTSymbol(symbol);
    const apiSymbol = symbol;
    const url = `${BITMART_API_URL}/spot/v1/ticker?symbol=${apiSymbol}`;

    try {
      const response = await axios.get(url);
      const tickers = response.data.data.tickers;
      
      if (!tickers || tickers.length === 0) {
        throw new Error(`No ticker data available for symbol: ${symbol}`);
      }

      const lastPrice = parseFloat(tickers[0].last_price);
      if (isNaN(lastPrice)) {
        throw new Error(`Invalid last price value for symbol: ${symbol}`);
      }

      // INTERNAL LOG: Old system success
      console.log(`[LEGACY_SYSTEM] ✅ OLD SYSTEM SUCCESS: ${symbol} = $${lastPrice} (BitMart API)`);
      
      return lastPrice;
    } catch (error: any) {
      // INTERNAL LOG: Old system error
      console.log(`[LEGACY_SYSTEM] ❌ OLD SYSTEM ERROR for ${symbol}: ${error.message}`);
      throw new Error(`Failed to fetch ticker data for ${symbol}`);
    }
  }

  /**
   * Fetches real-time price from multiple sources (Binance, CoinGecko, etc.)
   * @param symbol - The trading symbol in "BASE_QUOTE" format (e.g., "PWC_USDT").
   * @param userId - The user ID for logging
   * @returns The aggregated real-time price
   */
  public async fetchRealtimePrice(symbol: string, userId?: number): Promise<number> {
    const logger = userId ? getUserLogger(userId) : this.logger;
    
    try {
      // INTERNAL LOG: Price aggregation system activation
      console.log(`[PRICE_SYSTEM] 🚀 ACTIVATING MULTI-SOURCE PRICE AGGREGATION for ${symbol} (User: ${userId || 'system'})`);
      logger.log(`[PRICE_SYSTEM] Fetching real-time price for ${symbol} from multiple sources...`, 'info');
      
      // Get aggregated price from multiple sources
      const priceData = await this.priceAggregatorService.getAggregatedPrice(symbol, userId);
      
      // INTERNAL LOG: Success with detailed breakdown
      console.log(`[PRICE_SYSTEM] ✅ SUCCESS: ${symbol} = $${priceData.price} (Source: ${priceData.source})`);
      logger.log(`[PRICE_SYSTEM] Real-time price for ${symbol}: $${priceData.price} (from ${priceData.source})`, 'info');
      
      return priceData.price;
    } catch (error) {
      // INTERNAL LOG: Error in aggregation system
      console.log(`[PRICE_SYSTEM] ❌ ERROR in multi-source aggregation for ${symbol}: ${(error as Error).message}`);
      logger.error(`[PRICE_SYSTEM] Failed to fetch real-time price for ${symbol}: ${(error as Error).message}`);
      
      // Fallback to BitMart API if aggregation fails
      console.log(`[PRICE_SYSTEM] 🔄 FALLBACK: Switching to BitMart API for ${symbol}`);
      logger.log(`[PRICE_SYSTEM] Falling back to BitMart API for ${symbol}`, 'warn');
      return await this.fetchTicker(symbol);
    }
  }

  /**
   * Retrieves the available quantity for selling.
   * @param userId - The ID of the user.
   * @param symbol - The trading symbol in "BASE_QUOTE" format (e.g., "PWC_USDT").
   * @returns The available quantity.
   */
  public async getAvailableQuantity(
    userId: number,
    symbol: string,
  ): Promise<number> {
    try {
      const url = `${BITMART_API_URL}/account/v1/wallet`;
      const headers = await this.getAuthHeaders(
        userId,
        "/account/v1/wallet",
        "GET",
        "",
        {},
      );
      const response = await axios.get(url, { headers });

      const balances = response.data.data.wallet;
      const asset = balances.find(
        (b: any) =>
          b.currency.toUpperCase() === symbol.split("_")[0].toUpperCase(),
      );

      const available = asset ? parseFloat(asset.available) : 0;

      return available;
    } catch (error: unknown) {
      const err = error as any;
      throw new Error("Failed to fetch available quantity");
    }
  }

  private async getSymbolBalance(userId: number, symbol: string): Promise<number> {
    const baseCurrency = symbol.split('_')[0];
    return await this.getAvailableQuantity(userId, symbol);
  }

  /**
   * Floors a number to a specified precision.
   * @param value - The number to floor.
   * @param precision - The number of decimal places.
   * @returns The floored number.
   */
  private floorToPrecision(value: number, precision: number): number {
    const factor = Math.pow(10, precision);
    return Math.floor(value * factor) / factor;
  }
  /**
   * Places a buy or sell order.
   * @param userId - The ID of the user.
   * @param symbol - The trading symbol in "BASE_QUOTE" format (e.g., "PWC_USDT").
   * @param side - 'buy' or 'sell'.
   * @param amount - The amount to buy (notional) or sell (quantity).
   */
  public async placeOrder(
    userId: number,
    symbol: string,
    side: "buy" | "sell",
    amount: number = 0,
    fallbackPrice?: number // Optional parameter for fallback price
  ): Promise<void> {
    try {
      const { exchange } = await this.initializeExchanges(userId);
      const logger = getUserLogger(userId);
  
      const formattedSymbol = SymbolHelper.toCCXTSymbol(symbol);
      const market = exchange.markets[formattedSymbol];
      if (!market) {
        throw new Error(`Market info not found for symbol: ${formattedSymbol}`);
      }
  
      let payload: Record<string, any> = {};
      const parsedAmount = Number(amount);
  
      if (isNaN(parsedAmount) || parsedAmount <= 0) {
        throw new Error(`Invalid amount provided for ${side} order: ${amount}`);
      }
  
      if (side === "buy") {
        const baseCurrency = market.quote;
        if (!baseCurrency) {
          throw new Error("Unable to determine base currency from symbol.");
        }
  
        if (parsedAmount <= 0) {
          const balance = await this.getUserBalance(userId, baseCurrency);
          if (!Number.isFinite(balance) || balance <= 0) {
            throw new Error("Insufficient balance for buying.");
          }
          amount = balance;
        }
  
        const precision = market.precision.price || 8;
        const cost = parseFloat(parsedAmount.toFixed(precision));
        if (!Number.isFinite(cost) || cost <= 0) {
          throw new Error("Invalid cost calculated for buying.");
        }
  
        payload = {
          symbol: formattedSymbol,
          side: "buy",
          amount: cost,
        };
  
        logger.info(`[placeOrder][BUY] Payload: ${JSON.stringify(payload)}`);
        const order = await exchange.createMarketBuyOrder(formattedSymbol, cost);
  
        logger.info(
          `Market BUY order placed successfully: ${JSON.stringify(order)}`
        );
        if (!order || typeof order !== "object") {
          throw new Error("Invalid order response received from exchange.");
        }
      } else if (side === "sell") {
        try {
          const baseCurrency = symbol.split("_")[0];
          const state = this.getUserTradeState(userId);
  
          const availableQuantity = await this.getAvailableQuantity(
            userId,
            baseCurrency
          );
  
          if (availableQuantity <= 0) {
            logger.error(
              `Real-time available quantity for ${baseCurrency} is 0. Cannot proceed with sell order.`
            );
            return;
          }
  
          let currentPrice: number;
          try {
            currentPrice = await this.fetchTicker(symbol);
          } catch (error) {
            if (state.lastRecordedPrices[symbol]) {
              currentPrice = state.lastRecordedPrices[symbol];
              logger.warn(
                `Failed to fetch current price for ${symbol}. Using last recorded monitoring price: ${currentPrice}`
              );
            } else {
              throw new Error(
                `Failed to fetch ticker data for ${symbol} and no recorded price available.`
              );
            }
          }
  
          const estimatedSellValue = availableQuantity * currentPrice;
  
          if (estimatedSellValue < 1) {
            logger.error(
              `Sell value too small for ${symbol}. Available Quantity: ${availableQuantity}, Current Price: ${currentPrice}, Estimated Value: ${estimatedSellValue}`
            );
            throw new Error(
              `Sell order value is too small to process for ${symbol}.`
            );
          }
  
          payload = {
            symbol: formattedSymbol,
            side: "sell",
            amount: availableQuantity,
          };
          logger.info(`[placeOrder][SELL] Payload: ${JSON.stringify(payload)}`);
  
          const order = await exchange.createMarketSellOrder(
            formattedSymbol,
            availableQuantity
          );
          logger.info(
            `Market SELL order placed successfully: ${JSON.stringify(order)}`
          );
          if (!order || typeof order !== "object") {
            throw new Error("Invalid order response received from exchange.");
          }
        } catch (sellError: unknown) {
          const errorDetails = sellError as Error;
          logger.error(
            `Error placing sell order for ${symbol}: ${errorDetails.message}`,
            {
              stack: errorDetails.stack,
            }
          );
          throw errorDetails;
        }
      }
  
      if (!this.payloadLogs[symbol]) {
        this.payloadLogs[symbol] = [];
      }
      this.payloadLogs[symbol].push(payload);
    } catch (error: unknown) {
      const logger = getUserLogger(userId);
      const errorDetails = error as Error;
      logger.error(`Error placing ${side} order for ${symbol}: ${errorDetails.message}`, {
        stack: errorDetails.stack,
        symbol,
        side,
        amount,
      });
      throw new Error(
        `Failed to place ${side} order for ${symbol}: ${errorDetails.message}`
      );
    }
  }
  
  /**
   * Starts a trade by placing a buy order and initiating monitoring.
   * @param userId - The ID of the user.
   * @param symbol - The trading symbol in "BASE_QUOTE" format (e.g., "PWC_USDT").
   * @param amount - The amount to invest.
   * @param rebuyPercentage - The percentage to rebuy on conditions.
   * @param profitTarget - The target profit to achieve before stopping.
   * @param profitThresholds - Optional custom thresholds for this trade
   * @returns An object containing trade details.
   */
  public async startTrade(
    userId: number,
    symbol: string,
    amount: number,
    rebuyPercentage: number,
    profitTarget: number,
    userProfitCheckThreshold?: number,
    userLossCheckThreshold?: number,
    profitThresholds?: number[]
  ): Promise<any> {
    const logger = getUserLogger(userId);
    const state = this.getUserTradeState(userId);

    try {
      // Check if trading is paused (daily loss limit reached)
      if (state.tradingPausedUntil && Date.now() < state.tradingPausedUntil) {
        const remainingMinutes = Math.round((state.tradingPausedUntil - Date.now()) / 60000);
        throw new Error(
          `Trading is paused due to daily loss limit. Resumes in ${remainingMinutes} minutes. ` +
          `Daily P&L: ${state.accumulatedProfit.toFixed(2)} USDT`
        );
      }
      // Get user's saved thresholds from user entity
      const user = await this.userRepository.findOne({ where: { id: userId } });
      
      // Use saved thresholds, fallback to provided or defaults
      const effectiveProfitThreshold = userProfitCheckThreshold || 
        user?.profitThreshold || 
        0.008; // Default 0.8%

      const effectiveLossThreshold = userLossCheckThreshold || 
        user?.lossThreshold || 
        0.05;  // Default 5%

      logger.info(`Using thresholds for ${symbol}:`, {
        profitThreshold: `${(effectiveProfitThreshold * 100).toFixed(2)}%`,
        lossThreshold: `${(effectiveLossThreshold * 100).toFixed(2)}%`,
        source: user ? 'Saved Settings' : 'Defaults'
      });

      // Update state with correct thresholds
      state.profitCheckThreshold = effectiveProfitThreshold;
      state.lossCheckThreshold = effectiveLossThreshold;

      // Verify thresholds were set correctly
      logger.info(`Verified thresholds in state:`, {
        profitThreshold: `${(state.profitCheckThreshold * 100).toFixed(2)}%`,
        lossThreshold: `${(state.lossCheckThreshold * 100).toFixed(2)}%`
      });

      // Initialize state objects
      if (!state.purchasePrices) {
        state.purchasePrices = {};
      }
      if (!state.monitorIntervals) {
        state.monitorIntervals = {};
      }
      if (!state.activeTrades) {
        state.activeTrades = [];
      }

      logger.info(`Trade thresholds for ${symbol}:`, {
        profitTarget: `${(profitTarget * 100).toFixed(2)}%`,
        profitCheckThreshold: `${(state.profitCheckThreshold * 100).toFixed(2)}%`,
        lossCheckThreshold: `${(state.lossCheckThreshold * 100).toFixed(2)}%`,
        rebuyPercentage: `${rebuyPercentage}%`,
        profitThresholds: profitThresholds || [...state.profitThresholds]
      });

      // INTERNAL LOG: Price fetching for trade start
      console.log(`[TRADE_START] 🔍 FETCHING PRICE for trade start: ${symbol}`);
      
      const lastPrice = await this.fetchTicker(symbol);
      const purchaseQuantity = amount / lastPrice;
      
      // INTERNAL LOG: Trade start price confirmation
      console.log(`[TRADE_START] 💰 TRADE PRICE: ${symbol} = $${lastPrice} (Quantity: ${purchaseQuantity})`);

      // Place buy order
      await this.placeOrder(userId, symbol, "buy", amount);

      // Save purchase data BEFORE starting monitoring
      state.purchasePrices[symbol] = {
        price: lastPrice,
        timestamp: Date.now(),
        quantity: purchaseQuantity,
        rebuyPercentage,
        sold: false,
        profitThresholds: profitThresholds || [...state.profitThresholds],
      };

      // Save to active trades
      if (!state.activeTrades.includes(symbol)) {
        state.activeTrades.push(symbol);
      }

      // Update other state properties
      state.profitTarget = profitTarget;

      // Save state back to storage
      this.userTrades.set(userId, state);

      logger.info(`Purchase data saved for ${symbol} at price ${lastPrice}`);

      // Start WebSocket-based monitoring (with API fallback)
      await this.startWebSocketMonitoring(
        userId,
        symbol,
        purchaseQuantity,
        rebuyPercentage
      );

      return {
        symbol,
        amount,
        purchasePrice: lastPrice,
        purchaseQuantity,
        profitTarget,
        isMonitoring: true
      };

    } catch (error) {
      logger.error(`Error starting trade: ${(error as Error).message}`);
      throw error;
    }
  }
  

  /**
   * Calculates profit based on purchase price, quantity, and sell price.
   * @param symbol - The trading symbol.
   * @param quantity - The quantity bought.
   * @param sellPrice - The current sell price.
   * @returns The calculated profit.
   */
  private async calculateProfit(
    userId: number,
    symbol: string,
    quantity: number,
    sellPrice: number,
  ): Promise<number> {
    const state = this.getUserTradeState(userId);
    const purchase = state.purchasePrices[symbol];

    if (!purchase) {
      throw new Error("Purchase price not found");
    }

    const purchasePrice = purchase.price;
    const profit = (sellPrice - purchasePrice) * quantity;
    return profit;
  }

  /**
 * Checks if accumulated profit has reached the target and handles selling if necessary.
 * @param userId - The ID of the user.
 * @param symbol - The trading symbol.
 * @param quantity - The quantity bought.
 * @param sellPrice - The current sell price.
 */
  private async checkAndHandleProfit(
    userId: number,
    symbol: string,
    quantity: number,
    sellPrice: number,
  ): Promise<void> {
    const logger = getUserLogger(userId);
    const state = this.getUserTradeState(userId);
  
    try {
      logger.info(`[checkAndHandleProfit] Starting profit check for user: ${userId}, symbol: ${symbol}`);
      console.log(`[DEBUG][checkAndHandleProfit] Current state:`, {
        hasPurchasePrices: !!state.purchasePrices,
        symbolData: state.purchasePrices?.[symbol],
        quantity,
        sellPrice
      });
  
      // Initialize purchasePrices if it doesn't exist
      if (!state.purchasePrices) {
        state.purchasePrices = {};
      }
  
      const purchase = state.purchasePrices[symbol];
      if (!purchase) {
        console.log(`[DEBUG][checkAndHandleProfit] No purchase data found, attempting to recreate state`);
        
        // Recreate the purchase data if it's missing
        state.purchasePrices[symbol] = {
          price: sellPrice,  // Use the current sell price as the reference
          timestamp: Date.now(),
          quantity: quantity,
          sold: false,
          rebuyPercentage: 5, // Default value
          profitThresholds: [...state.profitThresholds]
        };
        
        console.log(`[DEBUG][checkAndHandleProfit] Recreated purchase data:`, state.purchasePrices[symbol]);
      }
  
      const purchasePrice = state.purchasePrices[symbol].price;
      const realizedProfit = (sellPrice - purchasePrice) * quantity;

      // Ensure profit values are calculated correctly
      const roundedProfit = parseFloat(realizedProfit.toFixed(2));
      if (isNaN(roundedProfit)) {
        logger.error(`[checkAndHandleProfit] Invalid profit calculated: ${realizedProfit}`);
        throw new Error("Invalid profit value calculated");
      }

      // Track if this was a profit or loss
      const wasLoss = roundedProfit < 0;
      const wasProfit = roundedProfit > 0;

      // Initialize per-symbol tracking if needed
      if (!state.consecutiveLossesBySymbol) {
        state.consecutiveLossesBySymbol = {};
      }
      if (!state.protectiveModeBySymbol) {
        state.protectiveModeBySymbol = {};
      }

      // Track per-symbol losses only (no global tracking)
      const symbolLosses = (state.consecutiveLossesBySymbol[symbol] || 0);
      
      // Update consecutive losses counter (per-symbol only)
      if (wasLoss) {
        state.consecutiveLossesBySymbol[symbol] = symbolLosses + 1;
        state.lastSellResult = 'loss';
        
        logger.warn(
          `[Loss Protection] Loss detected on ${symbol}: ${roundedProfit.toFixed(2)} USDT | ` +
          `Consecutive losses for ${symbol}: ${state.consecutiveLossesBySymbol[symbol]}`
        );
        
        // Gradient risk reduction based on per-symbol consecutive losses
        // 1st loss: Reduce position size to 75%
        // 2nd loss: Reduce to 50%, enter protective mode for this symbol only
        // 3rd loss: Same as 2nd (can add cooldown if needed)
        
        // Per-symbol protective mode after 2 losses on same symbol
        if (state.consecutiveLossesBySymbol[symbol] >= 2 && !state.protectiveModeBySymbol[symbol]) {
          state.protectiveModeBySymbol[symbol] = true;
          logger.warn(`[Loss Protection] ⚠️ PROTECTIVE MODE ACTIVATED for ${symbol} - 2 consecutive losses detected. Bot will be more conservative for this symbol only.`);
        }
      } else if (wasProfit) {
        // Reset consecutive losses on profit (per-symbol only)
        if (symbolLosses > 0) {
          logger.info(`[Loss Protection] Profit achieved on ${symbol} (${roundedProfit.toFixed(2)} USDT). Resetting consecutive losses counter for ${symbol}.`);
        }
        state.consecutiveLossesBySymbol[symbol] = 0;
        state.lastSellResult = 'profit';
        
        // Exit per-symbol protective mode
        if (state.protectiveModeBySymbol[symbol]) {
          state.protectiveModeBySymbol[symbol] = false;
          logger.info(`[Loss Protection] ✅ Protective mode deactivated for ${symbol} - Profitable trade achieved.`);
        }
      }

      // Update accumulated profit
      state.accumulatedProfit += roundedProfit;
      state.accumulatedProfit = parseFloat(state.accumulatedProfit.toFixed(2)); // Normalize to two decimals

      // Initialize starting equity on first trade if not set
      if (!state.startingEquity && state.accumulatedProfit === roundedProfit) {
        const currentBalance = await this.getUserBalance(userId);
        state.startingEquity = currentBalance + Math.abs(roundedProfit); // Approximate starting equity
        logger.info(`[Loss Protection] Starting equity set: ${state.startingEquity.toFixed(2)} USDT`);
      }

      // Check daily loss limit (e.g., -3% of starting equity)
      if (state.startingEquity && state.startingEquity > 0) {
        const dailyLossLimitPercent = -0.03; // -3% default
        const dailyLossLimit = state.startingEquity * dailyLossLimitPercent;
        const dailyPnL = state.accumulatedProfit; // Since startDayTimestamp
        
        if (dailyPnL <= dailyLossLimit) {
          const endOfDay = new Date();
          endOfDay.setHours(23, 59, 59, 999);
          state.tradingPausedUntil = endOfDay.getTime();
          
          logger.error(
            `[Loss Protection] 🛑 DAILY LOSS LIMIT REACHED - ` +
            `Daily P&L: ${dailyPnL.toFixed(2)} USDT (${((dailyPnL / state.startingEquity) * 100).toFixed(2)}%) ` +
            `exceeds limit: ${dailyLossLimit.toFixed(2)} USDT. Trading paused until end of day.`
          );
          
          // Stop all trading
          await this.stopTrade(userId);
          this.userTrades.set(userId, state);
          return;
        }
      }

      // Check if trading is paused
      if (state.tradingPausedUntil && Date.now() < state.tradingPausedUntil) {
        const remainingMinutes = Math.round((state.tradingPausedUntil - Date.now()) / 60000);
        logger.warn(`[Loss Protection] Trading is paused. Resumes in ${remainingMinutes} minutes.`);
        this.userTrades.set(userId, state);
        return;
      }

      console.log(`[DEBUG][checkAndHandleProfit] Profit calculation:`, {
        purchasePrice,
        sellPrice,
        quantity,
        realizedProfit: roundedProfit,
        accumulatedProfit: state.accumulatedProfit,
        consecutiveLosses: state.consecutiveLosses,
        protectiveMode: state.protectiveMode,
        dailyPnL: state.accumulatedProfit,
        dailyLossLimit: state.startingEquity ? (state.startingEquity * -0.03).toFixed(2) : 'N/A'
      });

      // Log updated profit
      logger.info(`Accumulated profit updated for user ${userId}: ${state.accumulatedProfit.toFixed(2)} USDT`);

      // Persist the updated state
      this.userTrades.set(userId, state);
  
      // Check if profit target is reached
      if (state.accumulatedProfit >= state.profitTarget) {
        logger.info(`Profit target of ${state.profitTarget} reached. Stopping trades.`);
        this.stopTrade(userId);
        return;
      }

      // Update this part if it exists
      const rebuyPercentage = state.purchasePrices[symbol]?.rebuyPercentage || 5;
      await this.startMonitoringAfterSale(userId, symbol, rebuyPercentage);
    } catch (error) {
      console.log(`[DEBUG][checkAndHandleProfit] Error:`, error);
      logger.error(
        `Error in checkAndHandleProfit for user ${userId}, symbol ${symbol}: ${(error as Error).message}`
      );
      throw error;
    }
  }  


  private isNewDay(state: UserTradeState): boolean {
    const oneDayInMillis = 24 * 60 * 60 * 1000;
    return Date.now() - state.startDayTimestamp >= oneDayInMillis;
  }

  /**
   * Starts continuous monitoring of the market for price changes.
   * @param userId - The ID of the user.
   * @param symbol - The trading symbol in "BASE_QUOTE" format.
   * @param quantity - The quantity bought.
   * @param rebuyPercentage - The percentage to rebuy on conditions.
   */
  private async startContinuousMonitoring(
    userId: number,
    symbol: string,
    quantity: number,
    rebuyPercentage: number,
  ): Promise<void> {
    const logger = getUserLogger(userId);
    const state = this.getUserTradeState(userId);
  
  try {
    // IMPORTANT: Clear ALL existing monitoring first
    if (state.afterSaleMonitorIntervals?.[symbol]) {
      clearInterval(state.afterSaleMonitorIntervals[symbol]);
      delete state.afterSaleMonitorIntervals[symbol];
      logger.info(`[startContinuousMonitoring] Forcefully cleared after-sale monitoring for ${symbol}`);
    }

    // Only clear and restart if monitoring doesn't exist or is different
    if (state.monitorIntervals?.[symbol]) {
      logger.warn(`[startContinuousMonitoring] Monitoring already exists for ${symbol}, clearing before restart`);
      clearInterval(state.monitorIntervals[symbol]);
      delete state.monitorIntervals[symbol];
    }

    // Verify monitoring is cleared
    if (state.afterSaleMonitorIntervals?.[symbol] || state.monitorIntervals?.[symbol]) {
      logger.warn(`[startContinuousMonitoring] Detected lingering monitors for ${symbol}. Force clearing.`);
      state.afterSaleMonitorIntervals = {};
      state.monitorIntervals = {};
    }

      // Start new monitoring
      state.monitorIntervals[symbol] = setInterval(async () => {
        try {
          // Try to use WebSocket price first (real-time), but check if it's stale
          let currentPrice: number;
          let priceSource: string;
          const lastWebSocketPrice = state.lastRecordedPrices[symbol];
          const lastUpdateTime = state.lastRecordedPrices[`${symbol}_timestamp`] || 0;
          const timeSinceUpdate = Date.now() - lastUpdateTime;
          const STALE_THRESHOLD = 5000; // Consider WebSocket data stale after 5 seconds
          
          // Use WebSocket price only if it exists AND is recent (less than 5 seconds old)
          if (lastWebSocketPrice && lastWebSocketPrice > 0 && timeSinceUpdate < STALE_THRESHOLD) {
            currentPrice = lastWebSocketPrice;
            priceSource = `WebSocket (real-time, ${Math.round(timeSinceUpdate / 1000)}s ago)`;
          } else {
            // WebSocket data is stale or missing - fetch fresh from API
            if (lastWebSocketPrice && timeSinceUpdate >= STALE_THRESHOLD) {
              logger.warn(`WebSocket data stale for ${symbol} (${Math.round(timeSinceUpdate / 1000)}s old), fetching fresh from API`);
            }
            currentPrice = await this.fetchTicker(symbol);
            priceSource = 'API (fresh)';
            // Update last recorded price for future use
            state.lastRecordedPrices[symbol] = currentPrice;
            state.lastRecordedPrices[`${symbol}_timestamp`] = Date.now();
          }
          
          const purchase = state.purchasePrices[symbol];

          if (!purchase || purchase.sold) {
            clearInterval(state.monitorIntervals[symbol]);
            delete state.monitorIntervals[symbol];
            logger.info(`Continuous monitoring stopped for ${symbol}`);
            return;
          }

          const purchasePrice = purchase.price;
          const priceChange = ((currentPrice - purchasePrice) / purchasePrice) * 100;

          // Separate profit and loss display with price source
          const statusLog = `${symbol} - Price: ${currentPrice} (${priceSource}) | Buy: ${purchasePrice} | ${
            priceChange >= 0 
              ? `Profit: ${priceChange.toFixed(2)}% | Loss: 0.00%`
              : `Profit: 0.00% | Loss: ${Math.abs(priceChange).toFixed(2)}%`
          } | Targets: +${(state.profitCheckThreshold * 100).toFixed(2)}% / -${(state.lossCheckThreshold * 100).toFixed(2)}%`;

          logger.info(statusLog);

          if (priceChange >= (state.profitCheckThreshold * 100)) {
            logger.info(`Profit target reached for ${symbol}. Selling.`);
            
            // Stop continuous monitoring before selling
            clearInterval(state.monitorIntervals[symbol]);
            delete state.monitorIntervals[symbol];
            
            await this.placeOrder(userId, symbol, "sell", quantity);
            await this.ensureSellCompleted(userId, symbol, quantity);
            await this.checkAndHandleProfit(userId, symbol, quantity, currentPrice);
            
            // Transition to monitoring after sale
            await this.startMonitoringAfterSale(userId, symbol, rebuyPercentage);
            return;
          } else if (Math.abs(priceChange) >= (state.lossCheckThreshold * 100)) {
            logger.info(`Loss threshold reached for ${symbol}. Selling.`);
            
            // Stop continuous monitoring before selling
            clearInterval(state.monitorIntervals[symbol]);
            delete state.monitorIntervals[symbol];
            
            await this.placeOrder(userId, symbol, "sell", quantity);
            await this.ensureSellCompleted(userId, symbol, quantity);
            await this.checkAndHandleProfit(userId, symbol, quantity, currentPrice);
            
            // Transition to monitoring after sale
            await this.startMonitoringAfterSale(userId, symbol, rebuyPercentage);
            return;
          }

        } catch (error) {
          logger.error(`Error in continuous monitoring for ${symbol}: ${error instanceof Error ? error.message : String(error)}`);
        }
      }, this.MONITORING_INTERVAL);

      // Save clean state
      this.userTrades.set(userId, state);
      logger.info(`[startContinuousMonitoring] Successfully started new monitoring for ${symbol} (interval: ${this.MONITORING_INTERVAL}ms)`);
    } catch (error) {
      logger.error(`Failed to start continuous monitoring for ${symbol}: ${error instanceof Error ? error.message : String(error)}`);
      throw error;
    }
  }

  private async startMonitoringAfterSale(
    userId: number,
    symbol: string,
    rebuyPercentage: number
  ): Promise<void> {
    const logger = getUserLogger(userId);
    const state = this.getUserTradeState(userId);
    const user = await this.userRepository.findOne({ where: { id: userId } });
    
    // Check if protective mode is active (per-symbol only)
    const isSymbolProtectiveMode = state.protectiveModeBySymbol?.[symbol] || false;
    const symbolLossCount = state.consecutiveLossesBySymbol?.[symbol] || 0;
    
    // Get user's configured thresholds or use defaults
    // User thresholds are stored as decimals (0.002 = 0.2%), convert to percentage for comparison
    const userProfitThreshold = user?.afterSaleProfitThreshold ?? 0.002; // Default 0.2%
    const userLossThreshold = user?.afterSaleLossThreshold ?? 0.0035; // Default 0.35%
    
    let profitThreshold = userProfitThreshold * 100;  // Convert to percentage (0.002 -> 0.2%)
    let lossThreshold = userLossThreshold * 100;      // Convert to percentage (0.0035 -> 0.35%)
    
    // Gradient risk reduction based on per-symbol consecutive losses
    let positionSizeMultiplier = 1.0; // Normal size
    
    // Adjust thresholds in protective mode - be more conservative (per-symbol only)
    if (isSymbolProtectiveMode) {
      // Gradient based on per-symbol consecutive losses
      if (symbolLossCount === 1) {
        // 1st loss: Slightly more conservative (1.5x threshold, 75% position)
        profitThreshold = profitThreshold * 1.5;
        positionSizeMultiplier = 0.75;
      } else if (symbolLossCount >= 2) {
        // 2+ losses: Full protective mode (2x threshold, 50% position, no loss rebuy)
        profitThreshold = profitThreshold * 2;
        positionSizeMultiplier = 0.5;
        lossThreshold = 999; // Disable loss-based rebuying
      }
      
      logger.warn(
        `[Loss Protection] 🛡️ PROTECTIVE MODE ACTIVE for ${symbol} - ` +
        `Consecutive losses: ${symbolLossCount}, ` +
        `Adjusted thresholds: +${profitThreshold.toFixed(2)}% profit ${lossThreshold >= 999 ? '(loss rebuy disabled)' : `/-${lossThreshold.toFixed(2)}%`}, ` +
        `Position size: ${(positionSizeMultiplier * 100).toFixed(0)}%`
      );
    }

    try {
      const initialPrice = await this.fetchTickerWithRetry(symbol);
      
      // Clear any existing after-sale monitoring first
      if (state.afterSaleMonitorIntervals?.[symbol]) {
        clearInterval(state.afterSaleMonitorIntervals[symbol]);
        delete state.afterSaleMonitorIntervals[symbol];
      }

      state.afterSaleMonitorIntervals[symbol] = setInterval(async () => {
        try {
          const currentPrice = await this.fetchTickerWithRetry(symbol);
          if (!currentPrice || isNaN(currentPrice)) return;

          const priceChange = ((currentPrice - initialPrice) / initialPrice) * 100;
          
          const isSymbolProtective = state.protectiveModeBySymbol?.[symbol] || false;
          const protectiveModeStatus = isSymbolProtective ? ' 🛡️ PROTECTIVE MODE' : '';
          logger.info(
            `After-Sale Monitor ${symbol}${protectiveModeStatus} - Current: ${currentPrice.toFixed(5)} | ` +
            `Initial: ${initialPrice.toFixed(5)} | Change: ${priceChange.toFixed(2)}% | ` +
            `Rebuy at: +${profitThreshold.toFixed(2)}% / ${lossThreshold < 999 ? `-${lossThreshold.toFixed(2)}%` : 'loss rebuy disabled'}`
          );

          // If rebuy conditions met, FIRST clear the interval, THEN execute rebuy
          if (this.shouldRebuy(priceChange, profitThreshold, lossThreshold)) {
            // Clear interval BEFORE executing rebuy
            clearInterval(state.afterSaleMonitorIntervals[symbol]);
            delete state.afterSaleMonitorIntervals[symbol];
            this.userTrades.set(userId, state);
            
            logger.info(`[startMonitoringAfterSale] Cleared monitoring before rebuy for ${symbol}`);
            
            // Small delay to ensure interval is cleared
            await new Promise(resolve => setTimeout(resolve, 100));
            
            // Now execute rebuy
            await this.executeRebuy(userId, symbol, currentPrice, rebuyPercentage);
            return;
          }
        } catch (error) {
          logger.error(`Error in after-sale monitoring for ${symbol}: ${error instanceof Error ? error.message : String(error)}`);
        }
      }, this.AFTER_SALE_MONITORING_INTERVAL);

      this.userTrades.set(userId, state);
      logger.info(`Started after-sale monitoring for ${symbol} (interval: ${this.AFTER_SALE_MONITORING_INTERVAL}ms)`);
    } catch (error) {
      logger.error(`Failed to start after-sale monitoring for ${symbol}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private async executeRebuy(
    userId: number,
    symbol: string,
    currentPrice: number,
    rebuyPercentage: number
  ): Promise<void> {
    const logger = getUserLogger(userId);
    const state = this.getUserTradeState(userId);

    try {
      // CRITICAL: Clear ALL monitoring before proceeding
      if (state.afterSaleMonitorIntervals?.[symbol]) {
        clearInterval(state.afterSaleMonitorIntervals[symbol]);
        delete state.afterSaleMonitorIntervals[symbol];
        // Clear the entire object to ensure no lingering references
        state.afterSaleMonitorIntervals = {};
        logger.info(`[executeRebuy] Forcefully cleared all after-sale monitoring`);
        
        // Save state immediately after clearing
        this.userTrades.set(userId, state);
        
        // Add delay to ensure interval is cleared
        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      // Check if in protective mode (per-symbol only) - apply gradient risk reduction
      const isSymbolProtectiveMode = state.protectiveModeBySymbol?.[symbol] || false;
      const symbolLossCount = state.consecutiveLossesBySymbol?.[symbol] || 0;
      
      let effectiveRebuyPercentage = rebuyPercentage;
      let positionSizeMultiplier = 1.0;
      
      if (isSymbolProtectiveMode) {
        // Gradient based on per-symbol consecutive losses
        if (symbolLossCount === 1) {
          positionSizeMultiplier = 0.75; // 75% position size
        } else if (symbolLossCount >= 2) {
          positionSizeMultiplier = 0.5; // 50% position size
        }
        
        effectiveRebuyPercentage = rebuyPercentage * positionSizeMultiplier;
        logger.warn(
          `[Loss Protection] 🛡️ Protective mode for ${symbol}: Reducing rebuy size from ${rebuyPercentage}% to ${effectiveRebuyPercentage.toFixed(1)}% ` +
          `(${(positionSizeMultiplier * 100).toFixed(0)}% of normal) - ${symbolLossCount} consecutive losses`
        );
      }

      const availableBalance = await this.getUserBalance(userId);
      const amountToRebuy = Math.max(5, (availableBalance * effectiveRebuyPercentage) / 100);

      if (amountToRebuy <= availableBalance) {
        await this.placeOrder(userId, symbol, "buy", amountToRebuy);
        logger.info(`Rebuy executed for ${symbol} - Amount: ${amountToRebuy} USDT`);

        // Update state
        state.purchasePrices[symbol] = {
          price: currentPrice,
          timestamp: Date.now(),
          quantity: amountToRebuy / currentPrice,
          sold: false,
          rebuyPercentage,
          profitThresholds: [...state.profitThresholds]
        };

        // Verify no after-sale monitoring exists before starting continuous
        if (state.afterSaleMonitorIntervals?.[symbol]) {
          logger.warn(`[executeRebuy] Detected lingering after-sale monitor, clearing again`);
          clearInterval(state.afterSaleMonitorIntervals[symbol]);
          state.afterSaleMonitorIntervals = {};
        }

        // Start new monitoring
        await this.startContinuousMonitoring(userId, symbol, amountToRebuy / currentPrice, rebuyPercentage);
      }
    } catch (error) {
      logger.error(`Failed to execute rebuy for ${symbol}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Waits for skyrocketing profit conditions and handles selling.
   * @param userId - The ID of the user.
   * @param symbol - The trading symbol.
   * @param quantity - The quantity bought.
   * @param rebuyPercentage - The percentage to rebuy on conditions.
   */
  private async waitForSkyrocketingProfit(
    userId: number,
    symbol: string,
    quantity: number,
    rebuyPercentage: number,
  ) {
    const logger = getUserLogger(userId); // Retrieve user-specific logger
    const state = this.getUserTradeState(userId); // Get user's state

    logger.info(`Monitoring skyrocketing profit for ${symbol}.`);

    const checkSkyrocketingProfit = setInterval(async () => {
      try {
        const currentPrice = await this.fetchTicker(symbol);
        const purchase = state.purchasePrices[symbol];
        const purchasePrice = purchase?.price;

        if (!purchasePrice) {
          clearInterval(checkSkyrocketingProfit);
          return;
        }

        const profit = (currentPrice - purchasePrice) / purchasePrice;

        if (profit >= 0.1) {
          // 10% profit
          logger.info(
            `Skyrocketing profit of 10% reached for ${symbol}. Selling.`,
          );
          await this.placeOrder(userId, symbol, "sell", quantity);
          this.stopTrade(userId);
          clearInterval(checkSkyrocketingProfit);
        }
      } catch (error) {
        logger.error(
          `Checking Coin ${(error as Error).message}`,
        );
      }
    }, 60000); // Check every 1 minute

    setTimeout(async () => clearInterval(checkSkyrocketingProfit), 240000); // Stop after 4 minutes
  }


public async stopTrade(userId: number): Promise<void> {
  const logger = getUserLogger(userId);
  const state = this.getUserTradeState(userId);

  try {
    logger.info(`Stopping all trading activities for user ${userId}.`);

    // Clear regular monitoring intervals
    for (const symbol in state.monitorIntervals) {
      clearInterval(state.monitorIntervals[symbol]);
      delete state.monitorIntervals[symbol];
      logger.info(`Cleared monitoring interval for ${symbol}`);
    }

    // Clear after-sale monitoring intervals
    if (state.afterSaleMonitorIntervals) {
      for (const symbol in state.afterSaleMonitorIntervals) {
        clearInterval(state.afterSaleMonitorIntervals[symbol]);
        delete state.afterSaleMonitorIntervals[symbol];
        logger.info(`Cleared after-sale monitoring interval for ${symbol}`);
      }
    }

    // Clear WebSocket subscriptions
    for (const symbol of state.activeTrades) {
      await this.unsubscribeFromWebSocketData(userId, symbol);
    }

    // Clear all state
    state.monitorIntervals = {};
    state.afterSaleMonitorIntervals = {};
    state.purchasePrices = {};
    state.profitTarget = 0;
    state.accumulatedProfit = 0;
    state.startDayTimestamp = Date.now();
    state.activeTrades = [];

    // Save cleared state
    this.userTrades.set(userId, state);

    logger.info('All trading activities stopped successfully');
  } catch (error) {
    logger.error(`Error stopping trading for user ${userId}: ${error instanceof Error ? error.message : String(error)}`);
    throw error;
  }
}

  /**
   * Retrieves the profit target.
   * @returns The profit target.
   */
  public getProfitTarget(userId: number): number {
    const state = this.getUserTradeState(userId);
    return state.profitTarget;
  }
  /**
   * Verifies if the user has an active subscription.
   * @param userEmail - The email of the user.
   */
  public async verifySubscription(userEmail: string): Promise<void> {
    const user = await this.userRepository.findByEmail(userEmail);
    if (!user || !user.has_subscription) {
      throw new UnauthorizedException(
        "You do not have an active subscription.",
      );
    }
  }

  /**
   * Retrieves the current status of trading activities.
   * @returns An object containing status details.
   */
  public getStatus(userId: number): Record<string, any> {
    const state = this.getUserTradeState(userId);
    const isProtectiveMode = state.protectiveMode && 
      (!state.protectiveModeUntil || Date.now() < state.protectiveModeUntil);
    
    return {
      activeTrades: Object.keys(state.purchasePrices).map((symbol) => ({
        symbol,
        monitoringStatus: state.purchasePrices[symbol]?.sold
          ? "Monitoring After Sale"
          : "Active",
      })),
      purchasePrices: state.purchasePrices,
      profitTarget: state.profitTarget,
      accumulatedProfit: state.accumulatedProfit,
      activeMonitoringIntervals: Object.keys(state.activeMonitoringIntervals),
      startDayTimestamp: new Date(state.startDayTimestamp).toISOString(),
      payloadLogs: state.payloadLogs,
      // Loss protection status (per-symbol only)
      lossProtection: {
        consecutiveLossesBySymbol: state.consecutiveLossesBySymbol || {},
        protectiveModeBySymbol: state.protectiveModeBySymbol || {},
        lastSellResult: state.lastSellResult,
        dailyPnL: state.accumulatedProfit,
        startingEquity: state.startingEquity,
        dailyLossLimit: state.startingEquity ? (state.startingEquity * -0.03).toFixed(2) : null,
        tradingPausedUntil: state.tradingPausedUntil 
          ? new Date(state.tradingPausedUntil).toISOString()
          : null,
      },
    };
  }

  /**
   * Reset loss protection (manually exit protective mode)
   * @param userId - The user ID
   * @param symbol - Optional symbol to reset. If not provided, resets all symbols
   */
  public resetLossProtection(userId: number, symbol?: string): void {
    const logger = getUserLogger(userId);
    const state = this.getUserTradeState(userId);
    
    if (symbol) {
      // Reset specific symbol only
      if (state.consecutiveLossesBySymbol) {
        state.consecutiveLossesBySymbol[symbol] = 0;
      }
      if (state.protectiveModeBySymbol) {
        state.protectiveModeBySymbol[symbol] = false;
      }
      logger.info(`[Loss Protection] Loss protection reset for ${symbol} by user ${userId}`);
    } else {
      // Reset all symbols
      state.consecutiveLossesBySymbol = {};
      state.protectiveModeBySymbol = {};
      state.lastSellResult = null;
      logger.info(`[Loss Protection] Loss protection reset for all symbols by user ${userId}`);
    }
    
    this.userTrades.set(userId, state);
  }
/**
 * Sells the specified coin immediately and transitions to monitoring after sale.
 * Stops continuous monitoring for the instance upon sell.
 * @param userId - The ID of the user.
 * @param symbol - The trading symbol in "BASE_QUOTE" format (e.g., "PWC_USDT").
 */
private async confirmSellAndStartMonitoring(
  userId: number,
  symbol: string,
  quantityToSell: number,
  rebuyPercentage: number
): Promise<void> {
  const logger = getUserLogger(userId);
  const state = this.getUserTradeState(userId);

  try {
    // Get remaining balance
    const remainingQuantity = await this.getSymbolBalance(userId, symbol);
    
    if (remainingQuantity < 0.1) {
      logger.info(`Sell confirmed for ${symbol}. Starting after-sale monitoring.`);
      
      // Update state
      if (state.purchasePrices[symbol]) {
        state.purchasePrices[symbol].sold = true;
        state.purchasePrices[symbol].quantity = 0;
      }

      // Start monitoring immediately
      await this.startMonitoringAfterSale(userId, symbol, rebuyPercentage);
      return;
    }

    throw new Error(`Sell not confirmed for ${symbol}. Remaining quantity: ${remainingQuantity}`);
  } catch (error) {
    logger.error(`Error confirming sell: ${(error as Error).message}`);
    throw error;
  }
}

public async sellNow(userId: number, symbol: string): Promise<void> {
  const logger = getUserLogger(userId);
  const state = this.getUserTradeState(userId);

  try {
    logger.info(`[sellNow] User ${userId} requested to sell ${symbol} immediately.`);

    // Get current purchase data before modifying state
    const purchase = state.purchasePrices[symbol];
    if (!purchase || purchase.quantity <= 0) {
      throw new Error(`No active trade found for symbol: ${symbol}`);
    }

    const quantityToSell = purchase.quantity;
    const rebuyPercentage = purchase.rebuyPercentage || 5;
    
    // Stop continuous monitoring first
    if (state.monitorIntervals[symbol]) {
      clearInterval(state.monitorIntervals[symbol]);
      delete state.monitorIntervals[symbol];
      logger.info(`[sellNow] Continuous monitoring stopped for ${symbol}.`);
    }

    // Place sell order
    await this.placeOrder(userId, symbol, "sell", quantityToSell);

    // Quick sell confirmation
    const remainingQuantity = await this.getSymbolBalance(userId, symbol);
    if (remainingQuantity < 0.1) {
      logger.info(`Sell confirmed for ${symbol}. Starting after-sale monitoring immediately.`);
      
      // Start monitoring before updating state
      await this.startMonitoringAfterSale(userId, symbol, rebuyPercentage);

      // Update state after monitoring is started
      state.purchasePrices[symbol] = {
        ...purchase,
        sold: true,
        quantity: 0
      };

      // Try to calculate profit
      try {
        const currentPrice = await this.fetchTickerWithRetry(symbol);
        if (currentPrice) {
          await this.checkAndHandleProfit(userId, symbol, quantityToSell, currentPrice);
        }
      } catch (error) {
        logger.warn(`Unable to calculate profit immediately: ${(error as Error).message}. Will retry during monitoring.`);
      }
    } else {
      throw new Error(`Sell not confirmed for ${symbol}. Remaining quantity: ${remainingQuantity}`);
    }

  } catch (error) {
    logger.error(`[sellNow] Error: ${(error as Error).message}`);
    throw error;
  }
}

public async buyNow(
  userId: number,
  symbol: string,
  percentage: number
): Promise<void> {
  const logger = getUserLogger(userId);
  const state = this.getUserTradeState(userId);

  try {
    // FIRST: Clear any existing monitoring
    if (state.afterSaleMonitorIntervals?.[symbol]) {
      clearInterval(state.afterSaleMonitorIntervals[symbol]);
      delete state.afterSaleMonitorIntervals[symbol];
      logger.info(`[buyNow] Cleared after-sale monitoring for ${symbol}`);
    }

    if (state.monitorIntervals?.[symbol]) {
      clearInterval(state.monitorIntervals[symbol]);
      delete state.monitorIntervals[symbol];
      logger.info(`[buyNow] Cleared existing monitoring for ${symbol}`);
    }

    const currentPrice = await this.fetchTickerWithRetry(symbol);
    const availableBalance = await this.getUserBalance(userId);
    const amount = (availableBalance * percentage) / 100;

    await this.placeOrder(userId, symbol, "buy", amount);
    
    // Update state with new purchase
    state.purchasePrices[symbol] = {
      price: currentPrice,
      timestamp: Date.now(),
      quantity: amount / currentPrice,
      sold: false,
      rebuyPercentage: percentage,
      profitThresholds: [...state.profitThresholds]
    };

    // Save state before starting new monitoring
    this.userTrades.set(userId, state);

    // Start WebSocket-based monitoring
    await this.startWebSocketMonitoring(userId, symbol, amount / currentPrice, percentage);
    
    logger.info(`Manual buy executed for ${symbol} - Amount: ${amount} USDT`);
  } catch (error) {
    logger.error(`Failed to execute manual buy for ${symbol}: ${error instanceof Error ? error.message : String(error)}`);
    throw error;
  }
}

private initializeTradeState(userId: number): UserTradeState {
  const newState: UserTradeState = {
    lastRecordedPrices: {},
    purchasePrices: {},
    profitTarget: 0,
    accumulatedProfit: 0,
    startDayTimestamp: Date.now(),
    payloadLogs: {},
    monitorIntervals: {},
    activeMonitoringIntervals: {},
    profitCheckThreshold: 0.008,    // Default 0.8% profit
    lossCheckThreshold: 0.006,      // Default 0.6% loss
    afterSaleProfitThreshold: 0.01,   // Default 1% profit for rebuy
    afterSaleLossThreshold: 0.01,    // Default 1% loss for rebuy
    profitThresholds: [...this.DEFAULT_PROFIT_THRESHOLDS],
    activeTrades: [],
    afterSaleMonitorIntervals: {},
    // Loss protection initialization
    consecutiveLosses: 0,
    protectiveMode: false,
    lastSellResult: null,
    protectiveModeUntil: undefined,
    consecutiveLossesBySymbol: {},
    protectiveModeBySymbol: {},
    startingEquity: undefined,
    dailyLossLimit: undefined,
    tradingPausedUntil: undefined,
  };
  this.userTradeStates.set(userId, newState);
  return newState;
}

public async setUserThresholds(
  userId: number,
  profitThreshold: number,
  lossThreshold: number
): Promise<void> {
  const logger = getUserLogger(userId);
  try {
    // Convert percentages to decimals if needed
    const normalizedProfitThreshold = profitThreshold > 1 ? profitThreshold / 100 : profitThreshold;
    const normalizedLossThreshold = lossThreshold > 1 ? lossThreshold / 100 : lossThreshold;

    // Find user first, then update
    const user = await this.userRepository.findOne({ where: { id: userId } });
    if (!user) throw new Error('User not found');

    user.profitThreshold = normalizedProfitThreshold;
    user.lossThreshold = normalizedLossThreshold;
    user.thresholds_updated_at = new Date();

    await this.userRepository.save(user);

    logger.info(`User thresholds saved:`, {
      profitThreshold: `${(normalizedProfitThreshold * 100).toFixed(2)}%`,
      lossThreshold: `${(normalizedLossThreshold * 100).toFixed(2)}%`
    });
  } catch (error) {
    logger.error(`Failed to save user thresholds: ${(error as Error).message}`);
    throw error;
  }
}

public async setAfterSaleThresholds(
  userId: number,
  profitThreshold: number,
  lossThreshold: number
): Promise<void> {
  const user = await this.userRepository.findOne({ where: { id: userId } });
  if (!user) {
    throw new Error('User not found');
  }

  // Convert to decimal if received as percentage
  const normalizedProfitThreshold = profitThreshold > 1 ? profitThreshold / 100 : profitThreshold;
  const normalizedLossThreshold = lossThreshold > 1 ? lossThreshold / 100 : lossThreshold;

  // Update user's after-sale thresholds
  user.afterSaleProfitThreshold = normalizedProfitThreshold;
  user.afterSaleLossThreshold = normalizedLossThreshold;

  // Save to database
  await this.userRepository.save(user);

  // Log the update
  this.logger.log(`Updated after-sale thresholds for user ${userId}:`, {
    profitThreshold: normalizedProfitThreshold,
    lossThreshold: normalizedLossThreshold
  });
}

private async fetchTickerWithRetry(
  symbol: string,
  maxRetries: number = 3,
  delayMs: number = 1000
): Promise<number> {
  const logger = getUserLogger(0);
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const price = await this.fetchTicker(symbol);
      if (price && !isNaN(price)) {
        return price;
      }
      throw new Error(`Invalid price received for ${symbol}`);
    } catch (error) {
      lastError = error as Error;
      logger.warn(`Attempt ${attempt}/${maxRetries} failed to fetch ticker for ${symbol}: ${(error as Error).message}`);
      if (attempt < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }
  throw new Error(`Failed to fetch ticker after ${maxRetries} attempts: ${lastError?.message}`);
}
public async getUserThresholds(userId: number) {
  const user = await this.userRepository.findOne({ where: { id: userId } });
  if (!user) {
    throw new Error('User not found');
  }
  
  return {
    profitThreshold: user.profitThreshold,
    lossThreshold: user.lossThreshold,
    afterSaleProfitThreshold: user.afterSaleProfitThreshold,
    afterSaleLossThreshold: user.afterSaleLossThreshold
  };
}
  
  private shouldRebuy(priceChange: number, profitThreshold: number, lossThreshold: number): boolean {
    // Only rebuy on profit if loss threshold is disabled (protective mode)
    if (lossThreshold >= 999) {
      return priceChange >= profitThreshold; // Only rebuy on profit
    }
    // Normal mode: rebuy on profit OR loss threshold
    return priceChange >= profitThreshold || Math.abs(priceChange) >= lossThreshold;
  }

  /**
   * Debug method to check WebSocket status
   */
  public async debugWebSocketStatus(userId: number, symbol: string): Promise<any> {
    const logger = getUserLogger(userId);
    const state = this.getUserTradeState(userId);
    
    logger.info(`[DEBUG] WebSocket Status Check for ${symbol}:`);
    logger.info(`[DEBUG] - WebSocket subscriptions: ${JSON.stringify(Array.from(this.websocketSubscriptions.get(symbol) || []))}`);
    logger.info(`[DEBUG] - Active monitoring intervals: ${Object.keys(state.monitorIntervals)}`);
    logger.info(`[DEBUG] - After-sale monitoring: ${Object.keys(state.afterSaleMonitorIntervals)}`);
    
    return {
      websocketSubscriptions: Array.from(this.websocketSubscriptions.get(symbol) || []),
      activeMonitoring: Object.keys(state.monitorIntervals),
      afterSaleMonitoring: Object.keys(state.afterSaleMonitorIntervals),
      lastRecordedPrice: state.lastRecordedPrices[symbol]
    };
  }

  /**
   * Ensure WebSocket subscription for price viewing (not trading)
   */
  public async ensureWebSocketSubscription(userId: number, symbol: string): Promise<void> {
    const logger = getUserLogger(userId);
    
    try {
      // Check if already subscribed
      const userSet = this.websocketSubscriptions.get(symbol);
      if (userSet && userSet.has(userId)) {
        logger.info(`Already subscribed to WebSocket data for ${symbol}`);
        return;
      }

      // Subscribe to WebSocket data for price viewing
      await this.subscribeToWebSocketData(userId, symbol);
      logger.info(`Subscribed to WebSocket data for price viewing: ${symbol}`);
    } catch (error) {
      logger.error(`Failed to ensure WebSocket subscription for ${symbol}: ${(error as Error).message}`);
      // Don't throw error, just log it - price viewing should still work with API fallback
    }
  }

/**
 * Set up WebSocket event listeners for real-time price monitoring
 */
private setupWebSocketListeners(): void {
  this.webSocketService.on('ticker', (symbol: string, tickerData: any) => {
    this.handleWebSocketTickerUpdate(symbol, tickerData);
  });

  this.webSocketService.on('trade', (symbol: string, tradeData: any) => {
    this.handleWebSocketTradeUpdate(symbol, tradeData);
  });

  this.webSocketService.on('disconnected', (symbol: string) => {
    this.handleWebSocketDisconnection(symbol);
  });

  this.webSocketService.on('reconnected', (symbol: string) => {
    this.handleWebSocketReconnection(symbol);
  });
}

  /**
   * Handle real-time ticker updates from WebSocket
   */
  private handleWebSocketTickerUpdate(symbol: string, tickerData: any): void {
    try {
      const price = parseFloat(tickerData.last_price);
      if (isNaN(price)) return;

      // INTERNAL LOG: WebSocket price update
      const timestamp = Date.now();
      console.log(`[WEBSOCKET] 📡 REAL-TIME UPDATE: ${symbol} = $${price} (BitMart WebSocket) at ${new Date(timestamp).toISOString()}`);

      // Update last recorded price for all users monitoring this symbol
      const userIds = this.websocketSubscriptions.get(symbol);
      if (userIds) {
        for (const userId of userIds) {
          const state = this.getUserTradeState(userId);
          const oldPrice = state.lastRecordedPrices[symbol];
          state.lastRecordedPrices[symbol] = price;
          state.lastRecordedPrices[`${symbol}_timestamp`] = timestamp; // Track when price was updated
          
          // Log price change if significant
          if (oldPrice && Math.abs(price - oldPrice) > 0.001) {
            const logger = getUserLogger(userId);
            logger.info(`[WebSocket] ${symbol} price updated: $${oldPrice} → $${price} (${((price - oldPrice) / oldPrice * 100).toFixed(3)}%)`);
          }
          
          // Check if this user has active trades for this symbol
          const purchase = state.purchasePrices[symbol];
          if (purchase && !purchase.sold) {
            this.processWebSocketPriceUpdate(userId, symbol, price, purchase);
          }
        }
      }
    } catch (error) {
      this.logger.error(`Error handling WebSocket ticker update for ${symbol}: ${(error as Error).message}`);
    }
  }

/**
 * Handle real-time trade updates from WebSocket
 */
private handleWebSocketTradeUpdate(symbol: string, tradeData: any): void {
  try {
    const price = parseFloat(tradeData.price);
    if (isNaN(price)) return;

    // Update last recorded price for all users monitoring this symbol
    const timestamp = Date.now();
    const userIds = this.websocketSubscriptions.get(symbol);
    if (userIds) {
      for (const userId of userIds) {
        const state = this.getUserTradeState(userId);
        const oldPrice = state.lastRecordedPrices[symbol];
        state.lastRecordedPrices[symbol] = price;
        state.lastRecordedPrices[`${symbol}_timestamp`] = timestamp; // Track when price was updated
        
        // Log price change if significant
        if (oldPrice && Math.abs(price - oldPrice) > 0.001) {
          const logger = getUserLogger(userId);
          logger.info(`[WebSocket Trade] ${symbol} price updated: $${oldPrice} → $${price} (${((price - oldPrice) / oldPrice * 100).toFixed(3)}%)`);
        }
        
        // Check if this user has active trades for this symbol
        const purchase = state.purchasePrices[symbol];
        if (purchase && !purchase.sold) {
          this.processWebSocketPriceUpdate(userId, symbol, price, purchase);
        }
      }
    }
  } catch (error) {
    this.logger.error(`Error handling WebSocket trade update for ${symbol}: ${(error as Error).message}`);
  }
}

/**
 * Process price updates from WebSocket for trading decisions
 */
private async processWebSocketPriceUpdate(
  userId: number,
  symbol: string,
  currentPrice: number,
  purchase: any
): Promise<void> {
  const logger = getUserLogger(userId);
  const state = this.getUserTradeState(userId);

  try {
    const purchasePrice = purchase.price;
    const priceChange = ((currentPrice - purchasePrice) / purchasePrice) * 100;

    // Log price update
    const statusLog = `${symbol} - Price: ${currentPrice} | Buy: ${purchasePrice} | ${
      priceChange >= 0 
        ? `Profit: ${priceChange.toFixed(2)}% | Loss: 0.00%`
        : `Profit: 0.00% | Loss: ${Math.abs(priceChange).toFixed(2)}%`
    } | Targets: +${(state.profitCheckThreshold * 100).toFixed(2)}% / -${(state.lossCheckThreshold * 100).toFixed(2)}%`;

    logger.info(statusLog);

    // Check profit/loss conditions
    if (priceChange >= (state.profitCheckThreshold * 100)) {
      logger.info(`Profit target reached for ${symbol} via WebSocket. Selling.`);
      await this.executeWebSocketSell(userId, symbol, currentPrice, purchase.quantity, purchase.rebuyPercentage);
    } else if (Math.abs(priceChange) >= (state.lossCheckThreshold * 100)) {
      logger.info(`Loss threshold reached for ${symbol} via WebSocket. Selling.`);
      await this.executeWebSocketSell(userId, symbol, currentPrice, purchase.quantity, purchase.rebuyPercentage);
    }
  } catch (error) {
    logger.error(`Error processing WebSocket price update for ${symbol}: ${(error as Error).message}`);
  }
}

/**
 * Execute sell order triggered by WebSocket price update
 */
private async executeWebSocketSell(
  userId: number,
  symbol: string,
  currentPrice: number,
  quantity: number,
  rebuyPercentage: number
): Promise<void> {
  const logger = getUserLogger(userId);
  const state = this.getUserTradeState(userId);

  try {
    // Stop any existing monitoring intervals
    if (state.monitorIntervals[symbol]) {
      clearInterval(state.monitorIntervals[symbol]);
      delete state.monitorIntervals[symbol];
    }

    // Place sell order
    await this.placeOrder(userId, symbol, "sell", quantity);
    
    // Ensure sell is completed
    await this.ensureSellCompleted(userId, symbol, quantity);
    
    // Calculate and handle profit
    await this.checkAndHandleProfit(userId, symbol, quantity, currentPrice);
    
    // Start after-sale monitoring
    await this.startMonitoringAfterSale(userId, symbol, rebuyPercentage);
    
    logger.info(`WebSocket-triggered sell completed for ${symbol}`);
  } catch (error) {
    logger.error(`Error executing WebSocket sell for ${symbol}: ${(error as Error).message}`);
  }
}

/**
 * Handle WebSocket disconnection
 */
private handleWebSocketDisconnection(symbol: string): void {
  this.logger.warn(`WebSocket disconnected for ${symbol}, falling back to API polling`);
  
  // Fall back to API polling for affected users
  const userIds = this.websocketSubscriptions.get(symbol);
  if (userIds) {
    for (const userId of userIds) {
      const state = this.getUserTradeState(userId);
      const purchase = state.purchasePrices[symbol];
      
      if (purchase && !purchase.sold) {
        // Only start continuous monitoring if it's not already running
        if (!state.monitorIntervals[symbol]) {
          this.logger.log(`Starting API polling fallback for ${symbol} (user ${userId})`);
          this.startContinuousMonitoring(userId, symbol, purchase.quantity, purchase.rebuyPercentage || 5).catch(error => {
            this.logger.error(`Failed to start continuous monitoring fallback for ${symbol}: ${(error as Error).message}`);
          });
        } else {
          this.logger.log(`API polling already running for ${symbol} (user ${userId}), skipping restart`);
        }
      }
    }
  }
}

/**
 * Handle WebSocket reconnection
 */
private handleWebSocketReconnection(symbol: string): void {
  this.logger.log(`WebSocket reconnected for ${symbol}, switching back to real-time monitoring`);
  
  // Stop API polling and switch back to WebSocket monitoring
  const userIds = this.websocketSubscriptions.get(symbol);
  if (userIds) {
    for (const userId of userIds) {
      const state = this.getUserTradeState(userId);
      
      // Stop continuous monitoring intervals (API polling fallback)
      if (state.monitorIntervals[symbol]) {
        clearInterval(state.monitorIntervals[symbol]);
        delete state.monitorIntervals[symbol];
        this.logger.log(`Stopped API polling for ${symbol} (user ${userId}), using WebSocket data`);
      }
    }
  }
}

/**
 * Subscribe to WebSocket data for a symbol
 */
private async subscribeToWebSocketData(userId: number, symbol: string): Promise<void> {
  try {
    // Add user to subscription tracking
    if (!this.websocketSubscriptions.has(symbol)) {
      this.websocketSubscriptions.set(symbol, new Set());
    }
    this.websocketSubscriptions.get(symbol)!.add(userId);

    // Subscribe to ticker and trade data
    await this.webSocketService.subscribeToTicker(symbol, userId);
    await this.webSocketService.subscribeToTrades(symbol, userId);
    
    this.logger.log(`Subscribed to WebSocket data for ${symbol} (user ${userId})`);
  } catch (error) {
    this.logger.error(`Failed to subscribe to WebSocket data for ${symbol}: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Unsubscribe from WebSocket data for a symbol
 */
private async unsubscribeFromWebSocketData(userId: number, symbol: string): Promise<void> {
  try {
    // Remove user from subscription tracking
    const userSet = this.websocketSubscriptions.get(symbol);
    if (userSet) {
      userSet.delete(userId);
      
      // If no more users are subscribed, unsubscribe from WebSocket
      if (userSet.size === 0) {
        await this.webSocketService.unsubscribe(symbol, 'ticker', userId);
        await this.webSocketService.unsubscribe(symbol, 'trades', userId);
        this.websocketSubscriptions.delete(symbol);
      }
    }
    
    this.logger.log(`Unsubscribed from WebSocket data for ${symbol} (user ${userId})`);
  } catch (error) {
    this.logger.error(`Failed to unsubscribe from WebSocket data for ${symbol}: ${(error as Error).message}`);
  }
}

/**
 * Start WebSocket-based continuous monitoring
 */
private async startWebSocketMonitoring(
  userId: number,
  symbol: string,
  quantity: number,
  rebuyPercentage: number
): Promise<void> {
  const logger = getUserLogger(userId);
  const state = this.getUserTradeState(userId);

  try {
    // Clear any existing API polling monitoring (but keep WebSocket subscription)
    if (state.monitorIntervals[symbol]) {
      clearInterval(state.monitorIntervals[symbol]);
      delete state.monitorIntervals[symbol];
      logger.debug(`Cleared API polling interval for ${symbol} before starting WebSocket`);
    }

    // Subscribe to WebSocket data
    await this.subscribeToWebSocketData(userId, symbol);
    
    logger.info(`Started WebSocket-based monitoring for ${symbol} (real-time price updates, backup check: ${this.BACKUP_API_CHECK_INTERVAL}ms)`);
    
    // Also start a backup API polling interval to ensure we have price data
    // This acts as a safety net if WebSocket data is delayed or connection is broken
    // Check more frequently (every 5 seconds) to catch stale data quickly
    const BACKUP_CHECK_INTERVAL = 5000; // Check every 5 seconds for stale data
    state.monitorIntervals[symbol] = setInterval(async () => {
      try {
        const purchase = state.purchasePrices[symbol];
        if (!purchase || purchase.sold) {
          // No active trade, stop monitoring
          clearInterval(state.monitorIntervals[symbol]);
          delete state.monitorIntervals[symbol];
          return;
        }

        const lastWebSocketPrice = state.lastRecordedPrices[symbol];
        const lastUpdateTime = state.lastRecordedPrices[`${symbol}_timestamp`] || 0;
        const timeSinceUpdate = Date.now() - lastUpdateTime;
        const STALE_THRESHOLD = 5000; // Consider stale after 5 seconds
        
        let currentPrice: number;
        let priceSource: string;
        
        // If WebSocket data is missing or stale, fetch fresh from API
        if (!lastWebSocketPrice || timeSinceUpdate > STALE_THRESHOLD) {
          const ageSeconds = Math.round(timeSinceUpdate / 1000);
          logger.debug(`[WebSocket Backup] Data stale/missing for ${symbol} (${ageSeconds}s old), fetching fresh from API`);
          currentPrice = await this.fetchTicker(symbol);
          state.lastRecordedPrices[symbol] = currentPrice;
          state.lastRecordedPrices[`${symbol}_timestamp`] = Date.now();
          priceSource = 'API (fresh)';
        } else {
          // Use fresh WebSocket data
          currentPrice = lastWebSocketPrice;
          priceSource = 'WebSocket (real-time)';
        }

        // Process the price update to show monitoring logs
        const purchasePrice = purchase.price;
        const priceChange = ((currentPrice - purchasePrice) / purchasePrice) * 100;

        // Log price update with profit/loss and thresholds
        const statusLog = `${symbol} - Price: ${currentPrice} (${priceSource}) | Buy: ${purchasePrice} | ${
          priceChange >= 0 
            ? `Profit: ${priceChange.toFixed(2)}% | Loss: 0.00%`
            : `Profit: 0.00% | Loss: ${Math.abs(priceChange).toFixed(2)}%`
        } | Targets: +${(state.profitCheckThreshold * 100).toFixed(2)}% / -${(state.lossCheckThreshold * 100).toFixed(2)}%`;

        logger.info(statusLog);

        // Check profit/loss conditions and execute sell if needed
        if (priceChange >= (state.profitCheckThreshold * 100)) {
          logger.info(`Profit target reached for ${symbol}. Selling.`);
          
          // Stop monitoring before selling
          clearInterval(state.monitorIntervals[symbol]);
          delete state.monitorIntervals[symbol];
          
          await this.placeOrder(userId, symbol, "sell", purchase.quantity);
          await this.ensureSellCompleted(userId, symbol, purchase.quantity);
          await this.checkAndHandleProfit(userId, symbol, purchase.quantity, currentPrice);
          
          // Transition to monitoring after sale
          await this.startMonitoringAfterSale(userId, symbol, purchase.rebuyPercentage || rebuyPercentage);
          return;
        } else if (Math.abs(priceChange) >= (state.lossCheckThreshold * 100)) {
          logger.info(`Loss threshold reached for ${symbol}. Selling.`);
          
          // Stop monitoring before selling
          clearInterval(state.monitorIntervals[symbol]);
          delete state.monitorIntervals[symbol];
          
          await this.placeOrder(userId, symbol, "sell", purchase.quantity);
          await this.ensureSellCompleted(userId, symbol, purchase.quantity);
          await this.checkAndHandleProfit(userId, symbol, purchase.quantity, currentPrice);
          
          // Transition to monitoring after sale
          await this.startMonitoringAfterSale(userId, symbol, purchase.rebuyPercentage || rebuyPercentage);
          return;
        }
      } catch (error) {
        logger.error(`Error in backup price check for ${symbol}: ${(error as Error).message}`);
      }
    }, BACKUP_CHECK_INTERVAL);
    
  } catch (error) {
    logger.error(`Failed to start WebSocket monitoring for ${symbol}: ${(error as Error).message}`);
    
    // Fall back to API polling
    logger.warn(`Falling back to API polling for ${symbol} (WebSocket unavailable)`);
    await this.startContinuousMonitoring(userId, symbol, quantity, rebuyPercentage);
  }
}
}