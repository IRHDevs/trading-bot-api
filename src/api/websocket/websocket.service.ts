// src/api/websocket/websocket.service.ts

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { getUserLogger } from '../logger';

interface WebSocketMessage {
  method: string;
  params: string[];
  id?: number;
}

interface TickerData {
  symbol: string;
  last_price: string;
  volume_24h: string;
  change_24h: string;
  timestamp: number;
}

interface TradeData {
  symbol: string;
  price: string;
  quantity: string;
  side: 'buy' | 'sell';
  timestamp: number;
}

interface WebSocketConnection {
  ws: WebSocket;
  subscriptions: Set<string>;
  isConnected: boolean;
  reconnectAttempts: number;
  maxReconnectAttempts: number;
  reconnectDelay: number;
}

@Injectable()
export class WebSocketService extends EventEmitter implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WebSocketService.name);
  private connections: Map<string, WebSocketConnection> = new Map();
  private readonly BITMART_WS_URL = 'wss://ws-manager-compress.bitmart.com?protocol=1.1';
  private readonly MAX_RECONNECT_ATTEMPTS = 5;
  private readonly RECONNECT_DELAY = 5000; // 5 seconds
  private readonly PING_INTERVAL = 30000; // 30 seconds
  private pingIntervals: Map<string, NodeJS.Timeout> = new Map();

  constructor() {
    super();
  }

  async onModuleInit() {
    this.logger.log('WebSocket Service initialized');
  }

  async onModuleDestroy() {
    this.logger.log('Closing all WebSocket connections...');
    for (const [symbol, connection] of this.connections) {
      this.closeConnection(symbol);
    }
    this.removeAllListeners();
  }

  /**
   * Subscribe to real-time ticker data for a symbol
   */
  async subscribeToTicker(symbol: string, userId?: number): Promise<void> {
    const logger = userId ? getUserLogger(userId) : this.logger;
    
    try {
      const connection = await this.getOrCreateConnection(symbol);
      
      if (connection.subscriptions.has(`ticker:${symbol}`)) {
        logger.warn(`Already subscribed to ticker for ${symbol}`);
        return;
      }

      const subscribeMessage: WebSocketMessage = {
        method: 'subscribe',
        params: [`spot/ticker:${symbol}`],
        id: Date.now()
      };

      if (connection.isConnected) {
        connection.ws.send(JSON.stringify(subscribeMessage));
        connection.subscriptions.add(`ticker:${symbol}`);
        logger.log(`Subscribed to ticker for ${symbol}`, 'info');
      } else {
        logger.error(`Cannot subscribe to ${symbol}: WebSocket not connected`);
      }
    } catch (error) {
      logger.error(`Failed to subscribe to ticker for ${symbol}: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Subscribe to real-time trade data for a symbol
   */
  async subscribeToTrades(symbol: string, userId?: number): Promise<void> {
    const logger = userId ? getUserLogger(userId) : this.logger;
    
    try {
      const connection = await this.getOrCreateConnection(symbol);
      
      if (connection.subscriptions.has(`trades:${symbol}`)) {
        logger.warn(`Already subscribed to trades for ${symbol}`);
        return;
      }

      const subscribeMessage: WebSocketMessage = {
        method: 'subscribe',
        params: [`spot/trade:${symbol}`],
        id: Date.now()
      };

      if (connection.isConnected) {
        connection.ws.send(JSON.stringify(subscribeMessage));
        connection.subscriptions.add(`trades:${symbol}`);
        logger.log(`Subscribed to trades for ${symbol}`, 'info');
      } else {
        logger.error(`Cannot subscribe to trades for ${symbol}: WebSocket not connected`);
      }
    } catch (error) {
      logger.error(`Failed to subscribe to trades for ${symbol}: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Subscribe to order book data for a symbol
   */
  async subscribeToOrderBook(symbol: string, userId?: number): Promise<void> {
    const logger = userId ? getUserLogger(userId) : this.logger;
    
    try {
      const connection = await this.getOrCreateConnection(symbol);
      
      if (connection.subscriptions.has(`orderbook:${symbol}`)) {
        logger.warn(`Already subscribed to order book for ${symbol}`);
        return;
      }

      const subscribeMessage: WebSocketMessage = {
        method: 'subscribe',
        params: [`spot/depth5:${symbol}`],
        id: Date.now()
      };

      if (connection.isConnected) {
        connection.ws.send(JSON.stringify(subscribeMessage));
        connection.subscriptions.add(`orderbook:${symbol}`);
        logger.log(`Subscribed to order book for ${symbol}`, 'info');
      } else {
        logger.error(`Cannot subscribe to order book for ${symbol}: WebSocket not connected`);
      }
    } catch (error) {
      logger.error(`Failed to subscribe to order book for ${symbol}: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Unsubscribe from a specific data stream
   */
  async unsubscribe(symbol: string, streamType: 'ticker' | 'trades' | 'orderbook', userId?: number): Promise<void> {
    const logger = userId ? getUserLogger(userId) : this.logger;
    
    try {
      const connection = this.connections.get(symbol);
      if (!connection || !connection.isConnected) {
        logger.warn(`No active connection for ${symbol}`);
        return;
      }

      const subscriptionKey = `${streamType}:${symbol}`;
      if (!connection.subscriptions.has(subscriptionKey)) {
        logger.warn(`Not subscribed to ${streamType} for ${symbol}`);
        return;
      }

      const unsubscribeMessage: WebSocketMessage = {
        method: 'unsubscribe',
        params: [`spot/${streamType === 'ticker' ? 'ticker' : streamType === 'trades' ? 'trade' : 'depth5'}:${symbol}`],
        id: Date.now()
      };

      connection.ws.send(JSON.stringify(unsubscribeMessage));
      connection.subscriptions.delete(subscriptionKey);
      logger.log(`Unsubscribed from ${streamType} for ${symbol}`, 'info');

      // Close connection if no more subscriptions
      if (connection.subscriptions.size === 0) {
        this.closeConnection(symbol);
      }
    } catch (error) {
      logger.error(`Failed to unsubscribe from ${streamType} for ${symbol}: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Get or create a WebSocket connection for a symbol
   */
  private async getOrCreateConnection(symbol: string): Promise<WebSocketConnection> {
    let connection = this.connections.get(symbol);
    
    if (!connection || !connection.isConnected) {
      connection = await this.createConnection(symbol);
      this.connections.set(symbol, connection);
    }
    
    return connection;
  }

  /**
   * Create a new WebSocket connection
   */
  private async createConnection(symbol: string): Promise<WebSocketConnection> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(this.BITMART_WS_URL);
      
      const connection: WebSocketConnection = {
        ws,
        subscriptions: new Set(),
        isConnected: false,
        reconnectAttempts: 0,
        maxReconnectAttempts: this.MAX_RECONNECT_ATTEMPTS,
        reconnectDelay: this.RECONNECT_DELAY
      };

      ws.on('open', () => {
        this.logger.log(`WebSocket connected for ${symbol}`);
        connection.isConnected = true;
        connection.reconnectAttempts = 0;
        
        // Start ping interval
        this.startPingInterval(symbol, connection);
        
        resolve(connection);
      });

      ws.on('message', (data: WebSocket.Data) => {
        try {
          const message = JSON.parse(data.toString());
          this.handleMessage(symbol, message);
        } catch (error) {
          this.logger.error(`Failed to parse WebSocket message for ${symbol}: ${(error as Error).message}`);
        }
      });

      ws.on('close', (code: number, reason: string) => {
        this.logger.warn(`WebSocket closed for ${symbol}: ${code} - ${reason}`);
        connection.isConnected = false;
        this.stopPingInterval(symbol);
        this.emit('disconnected', symbol, code, reason);
        
        // Attempt reconnection if not manually closed
        if (code !== 1000) {
          this.attemptReconnection(symbol, connection);
        }
      });

      ws.on('error', (error: Error) => {
        this.logger.error(`WebSocket error for ${symbol}: ${error.message}`);
        connection.isConnected = false;
        this.emit('error', symbol, error);
        
        // Attempt reconnection
        this.attemptReconnection(symbol, connection);
      });

      // Set timeout for connection
      setTimeout(() => {
        if (!connection.isConnected) {
          ws.close();
          reject(new Error(`WebSocket connection timeout for ${symbol}`));
        }
      }, 10000); // 10 second timeout
    });
  }

  /**
   * Handle incoming WebSocket messages
   */
  private handleMessage(symbol: string, message: any): void {
    try {
      if (message.data) {
        // Handle ticker data
        if (message.data.symbol && message.data.last_price) {
          const tickerData: TickerData = {
            symbol: message.data.symbol,
            last_price: message.data.last_price,
            volume_24h: message.data.volume_24h || '0',
            change_24h: message.data.change_24h || '0',
            timestamp: Date.now()
          };
          this.emit('ticker', symbol, tickerData);
        }
        
        // Handle trade data
        if (message.data.symbol && message.data.price && message.data.quantity) {
          const tradeData: TradeData = {
            symbol: message.data.symbol,
            price: message.data.price,
            quantity: message.data.quantity,
            side: message.data.side || 'buy',
            timestamp: Date.now()
          };
          this.emit('trade', symbol, tradeData);
        }
        
        // Handle order book data
        if (message.data.symbol && message.data.bids && message.data.asks) {
          this.emit('orderbook', symbol, message.data);
        }
      }
    } catch (error) {
      this.logger.error(`Error handling message for ${symbol}: ${(error as Error).message}`);
    }
  }

  /**
   * Attempt to reconnect a WebSocket connection
   */
  private async attemptReconnection(symbol: string, connection: WebSocketConnection): Promise<void> {
    if (connection.reconnectAttempts >= connection.maxReconnectAttempts) {
      this.logger.error(`Max reconnection attempts reached for ${symbol}`);
      this.connections.delete(symbol);
      this.emit('maxReconnectAttemptsReached', symbol);
      return;
    }

    connection.reconnectAttempts++;
    const delay = connection.reconnectDelay * Math.pow(2, connection.reconnectAttempts - 1);
    
    this.logger.log(`Attempting to reconnect ${symbol} in ${delay}ms (attempt ${connection.reconnectAttempts}/${connection.maxReconnectAttempts})`);
    
    setTimeout(async () => {
      try {
        const newConnection = await this.createConnection(symbol);
        newConnection.subscriptions = new Set(connection.subscriptions);
        this.connections.set(symbol, newConnection);
        
        // Resubscribe to all previous subscriptions
        for (const subscription of newConnection.subscriptions) {
          const [streamType, sym] = subscription.split(':');
          const subscribeMessage: WebSocketMessage = {
            method: 'subscribe',
            params: [`spot/${streamType === 'ticker' ? 'ticker' : streamType === 'trades' ? 'trade' : 'depth5'}:${sym}`],
            id: Date.now()
          };
          newConnection.ws.send(JSON.stringify(subscribeMessage));
        }
        
        this.logger.log(`Successfully reconnected ${symbol}`);
        this.emit('reconnected', symbol);
      } catch (error) {
        this.logger.error(`Reconnection failed for ${symbol}: ${(error as Error).message}`);
        this.attemptReconnection(symbol, connection);
      }
    }, delay);
  }

  /**
   * Start ping interval to keep connection alive
   */
  private startPingInterval(symbol: string, connection: WebSocketConnection): void {
    const interval = setInterval(() => {
      if (connection.isConnected) {
        connection.ws.ping();
      }
    }, this.PING_INTERVAL);
    
    this.pingIntervals.set(symbol, interval);
  }

  /**
   * Stop ping interval
   */
  private stopPingInterval(symbol: string): void {
    const interval = this.pingIntervals.get(symbol);
    if (interval) {
      clearInterval(interval);
      this.pingIntervals.delete(symbol);
    }
  }

  /**
   * Close a WebSocket connection
   */
  private closeConnection(symbol: string): void {
    const connection = this.connections.get(symbol);
    if (connection) {
      this.stopPingInterval(symbol);
      if (connection.isConnected) {
        connection.ws.close(1000, 'Manual close');
      }
      this.connections.delete(symbol);
      this.logger.log(`Closed WebSocket connection for ${symbol}`);
    }
  }

  /**
   * Get connection status for a symbol
   */
  getConnectionStatus(symbol: string): { isConnected: boolean; subscriptions: string[] } {
    const connection = this.connections.get(symbol);
    return {
      isConnected: connection?.isConnected || false,
      subscriptions: connection ? Array.from(connection.subscriptions) : []
    };
  }

  /**
   * Get all active connections
   */
  getAllConnections(): Map<string, { isConnected: boolean; subscriptions: string[] }> {
    const status = new Map();
    for (const [symbol, connection] of this.connections) {
      status.set(symbol, {
        isConnected: connection.isConnected,
        subscriptions: Array.from(connection.subscriptions)
      });
    }
    return status;
  }
}
