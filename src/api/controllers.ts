import {
  Controller,
  Get,
  Post,
  HttpException,
  HttpStatus,
  Query,
  Body,
  Req,
  UnauthorizedException,
  Delete,
  BadRequestException,
} from "@nestjs/common";
import { TradingService } from "./trading.service"; // Import TradingService
import AIService from "../ai/ai.service";
import {
  fetchTicker,
  getAIPredictions,
  getAIRecommendation,
  getOrderBook,
  getTicker,
} from "./api";
import { LogService } from "./log.service";
import {
  getMostVolatileCoin,
  getPumpedCoins,
  getTopGainers,
  getTopTrendingCoins,
  getTopTrendingCoinsForTheDay,
} from "./gainer";
import { UseGuards } from "@nestjs/common";
import { AuthGuard } from "./subscription/awt.guard";
import { ApiService } from "./api-service";
import { Request } from "express";
import { RequestWithUser } from "./request-user";
import * as jwt from "jsonwebtoken";
import { JwtService } from "@nestjs/jwt";

@Controller("trading")
export class TradingController {
  constructor(
    private readonly aiService: AIService,
    private readonly tradingService: TradingService, // Inject TradingService
    private readonly logService: LogService, // Inject LogService
    private readonly apiService: ApiService, // Inject ApiService
    private readonly jwtService: JwtService, // Inject JwtService
  ) {}

  @Get("ticker")
  async getTicker(@Query("symbol") symbol: string): Promise<any> {
    console.log("Received request for ticker with symbol:", symbol);
    if (!symbol) {
      console.error("Error: Symbol query parameter is required");
      throw new Error("Symbol query parameter is required");
    }
    try {
      const ticker = await getTicker(symbol);
      console.log("Ticker data:", ticker);
      return ticker;
    } catch (error) {
      console.error("Error fetching ticker:", error);
      throw error;
    }
  }

  @Get("realtime-ticker")
  @UseGuards(AuthGuard)
  async getRealtimeTicker(
    @Query("symbol") symbol: string,
    @Req() req: RequestWithUser
  ): Promise<any> {
    console.log("Received request for real-time ticker with symbol:", symbol);
    if (!symbol) {
      console.error("Error: Symbol query parameter is required");
      throw new Error("Symbol query parameter is required");
    }
    try {
      if (!req.user) {
        throw new Error("User not authenticated");
      }
      
      // Ensure WebSocket subscription for this symbol
      await this.tradingService.ensureWebSocketSubscription(req.user.id, symbol);
      
      // Get real-time price from WebSocket data, but check if it's stale
      const state = this.tradingService.getUserTradeState(req.user.id);
      const lastRecordedPrice = state.lastRecordedPrices[symbol];
      const lastUpdateTime = state.lastRecordedPrices[`${symbol}_timestamp`] || 0;
      const timeSinceUpdate = Date.now() - lastUpdateTime;
      const STALE_THRESHOLD = 5000; // Consider stale after 5 seconds
      
      // Use WebSocket data only if it exists AND is fresh (less than 5 seconds old)
      if (lastRecordedPrice && timeSinceUpdate < STALE_THRESHOLD) {
        const ageSeconds = Math.round(timeSinceUpdate / 1000);
        console.log(`Using fresh WebSocket data for ${symbol}: $${lastRecordedPrice} (${ageSeconds}s old)`);
        return {
          data: {
            tickers: [{
              symbol: symbol,
              last_price: lastRecordedPrice.toString(),
              volume_24h: "0",
              change_24h: "0"
            }]
          },
          source: 'websocket',
          timestamp: lastUpdateTime,
          age_seconds: ageSeconds
        };
      } else {
        // WebSocket data is stale or missing - fetch fresh from API
        const ageSeconds = lastRecordedPrice ? Math.round(timeSinceUpdate / 1000) : 'N/A';
        console.log(`WebSocket data stale/missing for ${symbol} (${ageSeconds}s old), fetching fresh from API`);
        const tickerData = await getTicker(symbol);
        const freshPrice = parseFloat(tickerData.data.tickers[0].last_price);
        
        // Update the cached price with fresh data
        state.lastRecordedPrices[symbol] = freshPrice;
        state.lastRecordedPrices[`${symbol}_timestamp`] = Date.now();
        
        return {
          ...tickerData,
          source: 'api_fresh',
          timestamp: Date.now(),
          was_stale: true,
          stale_age_seconds: ageSeconds
        };
      }
    } catch (error) {
      console.error("Error fetching real-time ticker:", error);
      throw error;
    }
  }

  @Get("realtime-price")
  @UseGuards(AuthGuard)
  async getRealtimePrice(
    @Query("symbol") symbol: string,
    @Req() req: RequestWithUser
  ): Promise<{ price: number; source: string; timestamp: number }> {
    console.log("Received request for real-time price with symbol:", symbol);
    if (!symbol) {
      console.error("Error: Symbol query parameter is required");
      throw new Error("Symbol query parameter is required");
    }
    try {
      if (!req.user) {
        throw new Error("User not authenticated");
      }
      
      // Get real-time price from multiple sources (Binance, CoinGecko, etc.)
      const price = await this.tradingService.fetchRealtimePrice(symbol, req.user.id);
      
      console.log("Real-time price for symbol:", symbol, "Price:", price);
      return {
        price: price,
        source: 'aggregated_multiple_sources',
        timestamp: Date.now()
      };
    } catch (error) {
      console.error("Error fetching real-time price:", error);
      throw error;
    }
  }

  @Get("order-book")
  async getOrderBook(@Query("symbol") symbol: string): Promise<any> {
    console.log("Received request for order book with symbol:", symbol);
    if (!symbol) {
      console.error("Error: Symbol query parameter is required");
      throw new Error("Symbol query parameter is required");
    }
    try {
      const orderBook = await getOrderBook(symbol);
      console.log("Order book data:", orderBook);
      return orderBook;
    } catch (error) {
      console.error("Error fetching order book:", error);
      throw error;
    }
  }

  @Get("ai-predictions")
  async getAIPredictions(@Query("symbol") symbol: string): Promise<any> {
    console.log("Received request for AI predictions with symbol:", symbol);
    if (!symbol) {
      console.error("Error: Symbol query parameter is required");
      throw new Error("Symbol query parameter is required");
    }
    try {
      const aiPredictions = await getAIPredictions(symbol);
      console.log("AI predictions data:", aiPredictions);
      return aiPredictions;
    } catch (error) {
      console.error("Error fetching AI predictions:", error);
      throw error;
    }
  }

  @Get("ai-recommendation")
  async getAIRecommendation(@Query("symbol") symbol: string): Promise<any> {
    console.log("Received request for AI recommendation with symbol:", symbol);
    if (!symbol) {
      console.error("Error: Symbol query parameter is required");
      throw new Error("Symbol query parameter is required");
    }
    try {
      const aiRecommendation = await getAIRecommendation(symbol);
      console.log("AI recommendation data:", aiRecommendation);
      return aiRecommendation;
    } catch (error) {
      console.error("Error fetching AI recommendation:", error);
      throw error;
    }
  }

  @Get("balance")
  async getBalance(@Req() req: Request): Promise<any> {
    // Log the headers to ensure the token is included
    console.log("Request headers:", req.headers);

    const authHeader = req.headers.authorization;

    // Check if the Authorization header is missing or malformed
    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      console.error("Authorization header is missing or malformed.");
      throw new HttpException(
        "Authorization token is missing or malformed",
        HttpStatus.UNAUTHORIZED,
      );
    }

    // Extract the token from the header
    const token = authHeader.split(" ")[1];

    // Log the extracted token
    console.log("Extracted token:", token);

    if (!token) {
      console.error("Token is missing.");
      throw new HttpException(
        "Authorization token is missing",
        HttpStatus.UNAUTHORIZED,
      );
    }

    try {
      const secretKey = process.env.JWT_SECRET || "your_secret_key";

      // Decode the token to get the payload
      const decoded = this.jwtService.verify(token, {
        secret: secretKey,
      }) as any;

      // Log the decoded payload
      console.log("Decoded token payload:", decoded);

      if (!decoded.email) {
        console.error("Decoded token is missing the email field.");
        throw new HttpException(
          "Invalid token: email is missing",
          HttpStatus.UNAUTHORIZED,
        );
      }

      const userId = decoded.id; // Assuming `id` is part of the JWT payload

      // Log the extracted user ID
      console.log("Extracted userId from token:", userId);

      if (!userId) {
        console.error("Decoded token is missing the user ID.");
        throw new HttpException(
          "Invalid token: user ID is missing",
          HttpStatus.UNAUTHORIZED,
        );
      }

      console.log(`Fetching balance for userId: ${userId}`);

      // Fetch the user balance
      const balance = await this.tradingService.getUserBalance(userId);

      // Log the fetched balance
      console.log(`Balance fetched for userId ${userId}:`, balance);

      return { balance };
    } catch (error) {
      const err = error as Error; // Explicitly cast to Error type
      console.error("Error fetching user balance:", err.message);
      console.error("Error stack trace:", err.stack);

      throw new HttpException(
        "Failed to fetch user balance",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get("top-gainer")
  async getTopGainer(@Query("symbols") symbols: string): Promise<any> {
    console.log("Received request for top gainer with symbols:", symbols);
    if (!symbols) {
      console.error("Error: Symbols query parameter is required");
      throw new Error("Symbols query parameter is required");
    }

    const symbolArray = symbols.split(",");
    console.log("Determining top gainer among symbols:", symbolArray);

    try {
      let topGainer = null;
      let maxGain = -Infinity;

      for (const symbol of symbolArray) {
        console.log("Processing symbol:", symbol);
        try {
          const tickerData = await getTicker(symbol);
          console.log("Ticker data for symbol", symbol, ":", tickerData);

          if (
            tickerData &&
            tickerData.data &&
            tickerData.data.tickers.length > 0
          ) {
            const ticker = tickerData.data.tickers[0];
            const openPrice = parseFloat(ticker.open_24h);
            const closePrice = parseFloat(ticker.close_24h);
            const gain = closePrice - openPrice;

            if (gain > maxGain) {
              maxGain = gain;
              topGainer = { symbol, gain };
            }
          }
        } catch (symbolError) {
          console.error(
            "Error fetching ticker data for symbol",
            symbol,
            ":",
            symbolError,
          );
        }
      }

      if (topGainer) {
        console.log("Top gainer determined:", topGainer);
        return { topGainer };
      } else {
        console.log("No valid symbols processed.");
        throw new Error("No valid symbols processed.");
      }
    } catch (error) {
      console.error("Error determining top gainer:", error);
      throw error;
    }
  }
  
  /**
   * Sets the user's default profit thresholds for trading.
   * @param req - The request object containing user information.
   * @param body.thresholds - Array of profit percentages at which to sell.
   */
  @UseGuards(AuthGuard)
  @Post("set-thresholds")
  async setTradeThresholds(
    @Req() req: RequestWithUser,
    @Body() body: {
      profitThreshold: number;  // Percentage as decimal
      lossThreshold: number;    // Percentage as decimal
    }
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new HttpException("Unauthorized", HttpStatus.UNAUTHORIZED);
    }

    try {
      this.tradingService.setUserThresholds(
        userId,
        body.profitThreshold,
        body.lossThreshold
      );
      return {
        message: 'Trading thresholds updated successfully',
        profitThreshold: `${(body.profitThreshold * 100).toFixed(2)}%`,
        lossThreshold: `${(body.lossThreshold * 100).toFixed(2)}%`
      };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      throw new HttpException(
        `Failed to set thresholds: ${errorMessage}`,
        HttpStatus.BAD_REQUEST
      );
    }
  }

  /**
   * Starts a trade with optional custom profit thresholds.
   */
  @UseGuards(AuthGuard)
  @Post("start-trade")
  async startTrade(
    @Req() req: RequestWithUser,
    @Body() body: {
      symbol: string;
      amount: number;
      rebuyPercentage: number;
      profitTarget: number;
      profitThresholds?: number[];
    }
  ): Promise<any> {
    const userId = req.user?.id;
    if (!userId) {
      throw new HttpException("Unauthorized", HttpStatus.UNAUTHORIZED);
    }

    const { symbol, amount, rebuyPercentage, profitTarget, profitThresholds } = body;

    try {
      const result = await this.tradingService.startTrade(
        userId,
        symbol,
        amount,
        rebuyPercentage,
        profitTarget,
        undefined, // default profit check threshold
        undefined, // default loss check threshold
        profitThresholds // optional custom thresholds
      );
      return { 
        message: `Trade started for ${symbol}`,
        ...result,
        profitThresholds: profitThresholds || 'using default thresholds'
      };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      throw new HttpException(
        `Failed to start trade: ${errorMessage}`,
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  /**
   * Validates and converts a value to a number.
   * @param value The value to validate and convert.
   * @param fieldName The name of the field (for error messages).
   * @param min The minimum allowed value (inclusive).
   * @param errorMessage The error message to throw if validation fails.
   * @param max Optional maximum allowed value (inclusive).
   * @returns The validated and converted number.
   */
  private validateAndConvertNumber(
    value: number | string,
    fieldName: string,
    min: number,
    errorMessage: string,
    max?: number,
  ): number {
    let numericValue: number;

    if (typeof value === "number") {
      numericValue = value;
    } else if (typeof value === "string") {
      numericValue = parseFloat(value);
      if (isNaN(numericValue)) {
        throw new BadRequestException(`${fieldName} must be a valid number.`);
      }
    } else {
      throw new BadRequestException(`${fieldName} must be a number.`);
    }

    if (numericValue < min) {
      throw new BadRequestException(errorMessage);
    }

    if (max !== undefined && numericValue > max) {
      throw new BadRequestException(errorMessage);
    }

    // Log the type and value for debugging
    console.log(
      `[startTrade] ${fieldName}:`,
      numericValue,
      `(${typeof numericValue})`,
    );

    return numericValue;
  }

  // trading.controller.ts

  @UseGuards(AuthGuard) // Apply AuthGuard
  @Get("stop-trade")
  async stopTrade(@Req() req: RequestWithUser): Promise<any> {
    const userId = req.user?.id; // Extract userId
    if (!userId) {
      throw new HttpException("Unauthorized", HttpStatus.UNAUTHORIZED);
    }

    console.log(`Received request to stop trading for userId: ${userId}`);

    try {
      this.tradingService.stopTrade(userId);
      return { message: "Trading stopped successfully" };
    } catch (error) {
      console.error("Error stopping trade:", error);
      throw new HttpException(
        "Failed to stop trade",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
  /**
   * Endpoint to place a buy or sell order.
   *
   * @param req - The request object containing user information.
   * @param body.symbol - The trading pair symbol in "PWC_USDT" format.
   * @param body.side - 'buy' or 'sell'.
   * @param body.amount - The amount to buy or sell.
   * @returns A success message or throws an error.
   */
  @UseGuards(AuthGuard) // Protect the endpoint
  @Post("place-order")
  async placeOrder(
    @Req() req: RequestWithUser,
    @Body() body: { symbol: string; side: "buy" | "sell"; amount?: number },
  ): Promise<any> {
    const userId = req.user?.id; // Extract userId
    if (!userId) {
      throw new HttpException("Unauthorized", HttpStatus.UNAUTHORIZED);
    }

    const { symbol, side, amount } = body;

    if (!symbol || !side) {
      throw new BadRequestException("Symbol and side are required.");
    }

    try {
      await this.tradingService.placeOrder(userId, symbol, side, amount);
      return { message: `Order ${side} placed for ${symbol}.` };
    } catch (error: unknown) {
      const err = error as Error;
      console.error(`[placeOrder] Error: ${err.message}`);
      throw new HttpException(
        `Failed to place order: ${err.message}`,
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get("fetch-ticker")
  async fetchTicker(@Query("symbol") symbol: string): Promise<number> {
    console.log("Received request to fetch ticker with symbol:", symbol);
    if (!symbol) {
      console.error("Error: Symbol query parameter is required");
      throw new HttpException(
        "Symbol query parameter is required",
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const lastPrice = await fetchTicker(symbol);
      console.log("Fetched ticker last price:", lastPrice);
      return lastPrice;
    } catch (error) {
      console.error("Error fetching ticker:", error);
      throw new HttpException(
        "Failed to fetch ticker data",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
  @UseGuards(AuthGuard)
@Get("accumulated-profit")
async getAccumulatedProfit(@Req() req: any): Promise<any> {
  console.log("Received request to retrieve accumulated profit");
  try {
    // Extract userId from the request
    const userId = req.user?.id;
    if (!userId) {
      throw new Error("User ID is missing from the request context");
    }

    const accumulatedProfit = this.tradingService.getAccumulatedProfit(userId);
    console.log(`Accumulated Profit for user ${userId}:`, accumulatedProfit);
    return { accumulatedProfit };
  } catch (error) {
    console.error("Error retrieving accumulated profit:", error);
    throw new HttpException(
      "Failed to retrieve accumulated profit",
      HttpStatus.INTERNAL_SERVER_ERROR,
    );
  }
}

  @UseGuards(AuthGuard)
  @Get("profit-target")
  async getProfitTarget(@Req() req: RequestWithUser): Promise<any> {
    console.log("Received request to retrieve profit target");
    const userId = req.user?.id;
    if (!userId) {
      throw new HttpException("Unauthorized", HttpStatus.UNAUTHORIZED);
    }

    try {
      const profitTarget = this.tradingService.getProfitTarget(userId);
      console.log("Current Profit Target:", profitTarget);
      return { profitTarget };
    } catch (error) {
      console.error("Error retrieving profit target:", error);
      throw new HttpException(
        "Failed to retrieve profit target",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
  @Get("top-gainers")
  async getTopGainers(): Promise<any> {
    console.log("Received request for top gainers");
    try {
      const topGainers = await getTopGainers();
      console.log("Top 5 Gainers in the last 3 minutes:", topGainers);
      return topGainers;
    } catch (error) {
      console.error("Error fetching top gainers:", error);
      throw new HttpException(
        "Failed to fetch top gainers",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
  @UseGuards(AuthGuard) // Apply guard
  @Get("most-volatile-coin")
  async getMostVolatileCoin(): Promise<any> {
    console.log("Received request for most volatile coin");
    try {
      const mostVolatileCoin = await getMostVolatileCoin();
      console.log("Most volatile coin data:", mostVolatileCoin);
      return { mostVolatileCoin };
    } catch (error) {
      console.error("Error fetching most volatile coin:", error);
      throw new HttpException(
        "Failed to fetch most volatile coin",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get("top-trending-coins")
  async getTopTrendingCoins(): Promise<any> {
    console.log("Received request for top trending coins");
    try {
      const trendingCoins = getTopTrendingCoins();
      console.log("Top trending coins:", trendingCoins);
      return trendingCoins;
    } catch (error) {
      console.error("Error fetching top trending coins:", error);
      throw new HttpException(
        "Failed to fetch top trending coins",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
  @Get("top-trending-coins-for-the-day")
  async getTopTrendingCoinsForTheDay(): Promise<any> {
    console.log("Received request for top trending coins for the day");
    try {
      const trendingCoinsForTheDay = getTopTrendingCoinsForTheDay();
      console.log("Top trending coins for the day:", trendingCoinsForTheDay);
      return { trendingCoinsForTheDay };
    } catch (error) {
      console.error("Error fetching top trending coins for the day:", error);
      throw new HttpException(
        "Failed to fetch top trending coins for the day",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get("pumped-coins")
  async getPumpedCoins(@Query("limit") limit: number): Promise<any> {
    console.log("Received request for pumped coins");
    try {
      const pumpedCoins = getPumpedCoins();

      const pumpedCoinsArray = Object.keys(pumpedCoins).map((symbol) => ({
        symbol,
        percentageIncrease: pumpedCoins[symbol].percentageIncrease,
      }));

      pumpedCoinsArray.sort(
        (a, b) => b.percentageIncrease - a.percentageIncrease,
      );

      const resultLimit = limit && limit > 0 ? Math.min(limit, 100) : 100;
      const limitedPumpedCoins = pumpedCoinsArray.slice(0, resultLimit);

      console.log("Pumped coins data:", limitedPumpedCoins);
      return limitedPumpedCoins;
    } catch (error) {
      console.error("Error fetching pumped coins:", error);
      throw new HttpException(
        "Failed to fetch pumped coins",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
  @Post("save-api-keys")
  async saveApiKeys(
    @Body()
    body: {
      bitmartApiKey: string;
      bitmartApiSecret: string;
      bitmartApiMemo: string;
      monitoringApiKey: string;
      monitoringApiSecret: string;
      monitoringApiMemo: string;
    },
    @Req() req: Request,
  ) {
    const {
      bitmartApiKey,
      bitmartApiSecret,
      bitmartApiMemo,
      monitoringApiKey,
      monitoringApiSecret,
      monitoringApiMemo,
    } = body;

    // Validate all required fields
    if (
      !bitmartApiKey ||
      !bitmartApiSecret ||
      !bitmartApiMemo ||
      !monitoringApiKey ||
      !monitoringApiSecret ||
      !monitoringApiMemo
    ) {
      throw new HttpException(
        "All API keys and memos are required",
        HttpStatus.BAD_REQUEST,
      );
    }

    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      throw new HttpException(
        "Authorization token is missing or malformed",
        HttpStatus.UNAUTHORIZED,
      );
    }

    const token = authHeader.split(" ")[1];

    if (!token) {
      throw new HttpException(
        "Authorization token is missing",
        HttpStatus.UNAUTHORIZED,
      );
    }

    try {
      const secretKey = process.env.JWT_SECRET || "your_secret_key";
      const decoded = jwt.verify(token, secretKey) as any;

      if (!decoded.email) {
        throw new HttpException(
          "Invalid token: email is missing",
          HttpStatus.UNAUTHORIZED,
        );
      }

      const userId = decoded.id; // Assuming `id` is part of the JWT payload
      if (!userId) {
        throw new HttpException(
          "Invalid token: user ID is missing",
          HttpStatus.UNAUTHORIZED,
        );
      }

      console.log("Saving API keys for userId:", userId);

      await this.apiService.saveKeysForUser(userId, {
        bitmartApiKey,
        bitmartApiSecret,
        bitmartApiMemo,
        monitoringApiKey,
        monitoringApiSecret,
        monitoringApiMemo,
      });

      return { message: "API keys saved successfully" };
    } catch (error) {
      console.error("Error saving API keys:", (error as Error).message);
      throw new HttpException(
        "Internal server error",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get("get-api-keys")
  async getApiKeys(@Req() req: Request) {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      throw new HttpException(
        "Authorization token is missing or malformed",
        HttpStatus.UNAUTHORIZED,
      );
    }

    const token = authHeader.split(" ")[1];

    if (!token) {
      throw new HttpException(
        "Authorization token is missing",
        HttpStatus.UNAUTHORIZED,
      );
    }

    try {
      const secretKey = process.env.JWT_SECRET || "your_secret_key";
      const decoded = jwt.verify(token, secretKey) as any;

      if (!decoded.email) {
        throw new HttpException(
          "Invalid token: email is missing",
          HttpStatus.UNAUTHORIZED,
        );
      }

      const userId = decoded.id; // Assuming `id` is part of the JWT payload
      if (!userId) {
        throw new HttpException(
          "Invalid token: user ID is missing",
          HttpStatus.UNAUTHORIZED,
        );
      }

      console.log(`Fetching API keys for userId: ${userId}`);

      const apiKeys = await this.apiService.getKeysForUser(userId);

      if (!apiKeys) {
        console.error(`No API keys found for userId: ${userId}`);
        throw new HttpException(
          "API keys not found for user",
          HttpStatus.NOT_FOUND,
        );
      }

      return { apiKeys };
    } catch (error) {
      console.error("Error fetching API keys:", (error as Error).message);
      throw new HttpException(
        "Failed to fetch API keys",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
  /**
 * Endpoint to trigger an immediate sell and stop continuous monitoring.
 *
 * @param req - The request object containing user information.
 * @param body.symbol - The trading pair symbol in "BASE_QUOTE" format (e.g., "BTC_USDT").
 * @returns A success message or throws an error.
 */
@UseGuards(AuthGuard) // Protect the endpoint
@Post("sell-now")
async sellNow(
  @Req() req: RequestWithUser,
  @Body() body: { symbol: string },
): Promise<any> {
  const userId = req.user?.id; // Extract userId
  if (!userId) {
    throw new HttpException("Unauthorized", HttpStatus.UNAUTHORIZED);
  }

  const { symbol } = body;

  if (!symbol) {
    throw new BadRequestException("Symbol is required.");
  }

  console.log(`Received request to sell now for userId: ${userId}, symbol: ${symbol}`);

  try {
    await this.tradingService.sellNow(userId, symbol);
    return { message: `Sell order placed for ${symbol}, monitoring stopped.` };
  } catch (error) {
    console.error(`[sellNow] Error processing sell-now request for ${symbol}:`, error);
    throw new HttpException(
      `Failed to process sell-now request: ${(error as Error).message}`,
      HttpStatus.INTERNAL_SERVER_ERROR,
    );
  }
}

  @UseGuards(AuthGuard)
  @Get("status")
  getStatus(@Req() req: RequestWithUser): any {
    const userId = req.user?.id;
    if (!userId) {
      throw new HttpException("Unauthorized", HttpStatus.UNAUTHORIZED);
    }
    return this.tradingService.getStatus(userId);
  }
  
  @Post("delete-api-keys")
  async deleteApiKeys(@Req() req: Request): Promise<{ message: string }> {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      throw new HttpException(
        "Authorization token is missing or malformed",
        HttpStatus.UNAUTHORIZED,
      );
    }

    const token = authHeader.split(" ")[1];

    if (!token) {
      throw new HttpException(
        "Authorization token is missing",
        HttpStatus.UNAUTHORIZED,
      );
    }

    try {
      const secretKey = process.env.JWT_SECRET || "your_secret_key";
      const decoded = jwt.verify(token, secretKey) as any;

      if (!decoded.email) {
        throw new HttpException(
          "Invalid token: email is missing",
          HttpStatus.UNAUTHORIZED,
        );
      }

      const userId = decoded.id; // Assuming `id` is part of the JWT payload
      if (!userId) {
        throw new HttpException(
          "Invalid token: user ID is missing",
          HttpStatus.UNAUTHORIZED,
        );
      }

      console.log(`Deleting API keys for userId: ${userId}`);

      await this.apiService.deleteKeysForUser(userId);

      return { message: "API keys deleted successfully" };
    } catch (error) {
      console.error("Error deleting API keys:", (error as Error).message);
      throw new HttpException(
        "Failed to delete API keys",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
  @UseGuards(AuthGuard) // Guard to ensure user is authenticated
@Post("buy-now")
async buyNow(
  @Req() req: RequestWithUser,
  @Body() body: { symbol: string; usdtAmount?: number }, // Make usdtAmount optional
): Promise<any> {
  const userId = req.user?.id;
  if (!userId) {
    throw new HttpException("Unauthorized", HttpStatus.UNAUTHORIZED);
  }

  // Destructure the values
  const { symbol, usdtAmount } = body;

  // Still require the symbol
  if (!symbol) {
    throw new BadRequestException("Symbol is required.");
  }

  // (No more 'usdtAmount must be positive' check.)
  console.log(
    `Received request to buy now for userId: ${userId}, ` +
    `symbol: ${symbol}, amount: ${usdtAmount} USDT (ignored if 0)`,
  );

  try {
    // Pass 0 or the raw value. The service logic will do (balance * rebuyPercentage / 100).
    await this.tradingService.buyNow(userId, symbol, usdtAmount || 0);

    return { message: `Buy order placed for ${symbol}, monitoring started.` };
  } catch (error) {
    console.error(`[buyNow] Error processing buy-now request for ${symbol}:`, error);
    throw new HttpException(
      `Failed to process buy-now request: ${(error as Error).message}`,
      HttpStatus.INTERNAL_SERVER_ERROR,
    );
  }
}

  @UseGuards(AuthGuard)
  @Post("set-after-sale-thresholds")
  async setAfterSaleThresholds(
    @Req() req: RequestWithUser,
    @Body() body: {
      profitThreshold: number;
      lossThreshold: number;
    }
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new HttpException("Unauthorized", HttpStatus.UNAUTHORIZED);
    }

    try {
      this.tradingService.setAfterSaleThresholds(
        userId,
        body.profitThreshold,
        body.lossThreshold
      );
      return {
        message: 'After-sale thresholds updated successfully',
        profitThreshold: `${(body.profitThreshold * 100).toFixed(2)}%`,
        lossThreshold: `${(body.lossThreshold * 100).toFixed(2)}%`
      };
    } catch (error: unknown) {
      throw new HttpException(
        `Failed to set after-sale thresholds: ${(error as Error).message}`,
        HttpStatus.BAD_REQUEST
      );
    }
  }

  @UseGuards(AuthGuard)
  @Get("get-thresholds")
  async getTradeThresholds(@Req() req: RequestWithUser) {
    const userId = req.user?.id;
    if (!userId) {
      throw new HttpException("Unauthorized", HttpStatus.UNAUTHORIZED);
    }

    try {
      const thresholds = await this.tradingService.getUserThresholds(userId);
      return {
        message: 'Trading thresholds retrieved successfully',
        profitThreshold: `${(thresholds.profitThreshold * 100).toFixed(2)}%`,
        lossThreshold: `${(thresholds.lossThreshold * 100).toFixed(2)}%`,
        afterSaleProfitThreshold: `${(thresholds.afterSaleProfitThreshold * 100).toFixed(2)}%`,
        afterSaleLossThreshold: `${(thresholds.afterSaleLossThreshold * 100).toFixed(2)}%`
      };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      throw new HttpException(
        `Failed to get thresholds: ${errorMessage}`,
        HttpStatus.BAD_REQUEST
      );
    }
  }
}