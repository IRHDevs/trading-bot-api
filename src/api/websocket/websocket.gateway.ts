// src/api/websocket/websocket.gateway.ts

import {
  WebSocketGateway as NestWebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  MessageBody,
  ConnectedSocket,
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { Logger, UseGuards } from '@nestjs/common';
import { WebSocketService } from './websocket.service';
import { JwtService } from '@nestjs/jwt';
import { UserRepository } from '../user/user-repository';

interface AuthenticatedSocket extends Socket {
  userId?: number;
}

@NestWebSocketGateway({
  cors: {
    origin: process.env.FRONTEND_URL || 'https://tradingbot.ascarinet.com',
    credentials: true,
  },
  namespace: '/trading',
})
export class WebSocketGateway
  implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect
{
  @WebSocketServer()
  server!: Server;

  private readonly logger = new Logger(WebSocketGateway.name);
  private connectedClients: Map<string, AuthenticatedSocket> = new Map();

  constructor(
    private readonly webSocketService: WebSocketService,
    private readonly jwtService: JwtService,
    private readonly userRepository: UserRepository,
  ) {}

  afterInit(server: Server) {
    this.logger.log('WebSocket Gateway initialized');
    
    // Set up event listeners for market data
    this.webSocketService.on('ticker', (symbol: string, data: any) => {
      this.broadcastToSubscribers('ticker', symbol, data);
    });

    this.webSocketService.on('trade', (symbol: string, data: any) => {
      this.broadcastToSubscribers('trade', symbol, data);
    });

    this.webSocketService.on('orderbook', (symbol: string, data: any) => {
      this.broadcastToSubscribers('orderbook', symbol, data);
    });

    this.webSocketService.on('disconnected', (symbol: string, code: number, reason: string) => {
      this.broadcastToSubscribers('connection_status', symbol, {
        status: 'disconnected',
        code,
        reason,
      });
    });

    this.webSocketService.on('reconnected', (symbol: string) => {
      this.broadcastToSubscribers('connection_status', symbol, {
        status: 'reconnected',
      });
    });
  }

  async handleConnection(client: AuthenticatedSocket) {
    try {
      // Extract token from handshake
      const token = client.handshake.auth?.token || client.handshake.headers?.authorization?.replace('Bearer ', '');
      
      if (!token) {
        this.logger.warn(`Client ${client.id} connected without token`);
        client.emit('error', { message: 'Authentication required' });
        client.disconnect();
        return;
      }

      // Verify JWT token
      try {
        const payload = this.jwtService.verify(token);
        const user = await this.userRepository.findOne({ where: { id: payload.sub } });
        
        if (!user) {
          this.logger.warn(`Client ${client.id} with invalid user ID: ${payload.sub}`);
          client.emit('error', { message: 'User not found' });
          client.disconnect();
          return;
        }

        client.userId = user.id;
        this.connectedClients.set(client.id, client);
        
        this.logger.log(`Client ${client.id} connected for user ${user.id}`);
        client.emit('connected', { userId: user.id, message: 'Successfully connected' });
      } catch (error) {
        this.logger.warn(`Client ${client.id} with invalid token: ${(error as Error).message}`);
        client.emit('error', { message: 'Invalid token' });
        client.disconnect();
      }
    } catch (error) {
      this.logger.error(`Error handling connection for client ${client.id}: ${(error as Error).message}`);
      client.emit('error', { message: 'Connection error' });
      client.disconnect();
    }
  }

  handleDisconnect(client: AuthenticatedSocket) {
    this.logger.log(`Client ${client.id} disconnected`);
    this.connectedClients.delete(client.id);
  }

  @SubscribeMessage('subscribe_ticker')
  async handleSubscribeTicker(
    @ConnectedSocket() client: AuthenticatedSocket,
    @MessageBody() data: { symbol: string },
  ) {
    if (!client.userId) {
      client.emit('error', { message: 'Authentication required' });
      return;
    }

    try {
      await this.webSocketService.subscribeToTicker(data.symbol, client.userId);
      client.emit('subscribed', { type: 'ticker', symbol: data.symbol });
      this.logger.log(`User ${client.userId} subscribed to ticker for ${data.symbol}`);
    } catch (error) {
      client.emit('error', { message: `Failed to subscribe to ticker: ${(error as Error).message}` });
    }
  }

  @SubscribeMessage('subscribe_trades')
  async handleSubscribeTrades(
    @ConnectedSocket() client: AuthenticatedSocket,
    @MessageBody() data: { symbol: string },
  ) {
    if (!client.userId) {
      client.emit('error', { message: 'Authentication required' });
      return;
    }

    try {
      await this.webSocketService.subscribeToTrades(data.symbol, client.userId);
      client.emit('subscribed', { type: 'trades', symbol: data.symbol });
      this.logger.log(`User ${client.userId} subscribed to trades for ${data.symbol}`);
    } catch (error) {
      client.emit('error', { message: `Failed to subscribe to trades: ${(error as Error).message}` });
    }
  }

  @SubscribeMessage('subscribe_orderbook')
  async handleSubscribeOrderBook(
    @ConnectedSocket() client: AuthenticatedSocket,
    @MessageBody() data: { symbol: string },
  ) {
    if (!client.userId) {
      client.emit('error', { message: 'Authentication required' });
      return;
    }

    try {
      await this.webSocketService.subscribeToOrderBook(data.symbol, client.userId);
      client.emit('subscribed', { type: 'orderbook', symbol: data.symbol });
      this.logger.log(`User ${client.userId} subscribed to order book for ${data.symbol}`);
    } catch (error) {
      client.emit('error', { message: `Failed to subscribe to order book: ${(error as Error).message}` });
    }
  }

  @SubscribeMessage('unsubscribe')
  async handleUnsubscribe(
    @ConnectedSocket() client: AuthenticatedSocket,
    @MessageBody() data: { symbol: string; type: 'ticker' | 'trades' | 'orderbook' },
  ) {
    if (!client.userId) {
      client.emit('error', { message: 'Authentication required' });
      return;
    }

    try {
      await this.webSocketService.unsubscribe(data.symbol, data.type, client.userId);
      client.emit('unsubscribed', { type: data.type, symbol: data.symbol });
      this.logger.log(`User ${client.userId} unsubscribed from ${data.type} for ${data.symbol}`);
    } catch (error) {
      client.emit('error', { message: `Failed to unsubscribe: ${(error as Error).message}` });
    }
  }

  @SubscribeMessage('get_connection_status')
  async handleGetConnectionStatus(
    @ConnectedSocket() client: AuthenticatedSocket,
    @MessageBody() data: { symbol: string },
  ) {
    if (!client.userId) {
      client.emit('error', { message: 'Authentication required' });
      return;
    }

    try {
      const status = this.webSocketService.getConnectionStatus(data.symbol);
      client.emit('connection_status', { symbol: data.symbol, ...status });
    } catch (error) {
      client.emit('error', { message: `Failed to get connection status: ${(error as Error).message}` });
    }
  }

  @SubscribeMessage('get_all_connections')
  async handleGetAllConnections(@ConnectedSocket() client: AuthenticatedSocket) {
    if (!client.userId) {
      client.emit('error', { message: 'Authentication required' });
      return;
    }

    try {
      const connections = this.webSocketService.getAllConnections();
      client.emit('all_connections', Object.fromEntries(connections));
    } catch (error) {
      client.emit('error', { message: `Failed to get connections: ${(error as Error).message}` });
    }
  }

  /**
   * Broadcast market data to all connected clients
   */
  private broadcastToSubscribers(event: string, symbol: string, data: any) {
    this.server.emit(event, { symbol, data, timestamp: Date.now() });
  }

  /**
   * Send data to a specific user
   */
  sendToUser(userId: number, event: string, data: any) {
    for (const [clientId, client] of this.connectedClients) {
      if (client.userId === userId) {
        client.emit(event, data);
      }
    }
  }

  /**
   * Send data to all connected clients
   */
  broadcast(event: string, data: any) {
    this.server.emit(event, data);
  }

  /**
   * Get connected clients count
   */
  getConnectedClientsCount(): number {
    return this.connectedClients.size;
  }

  /**
   * Get connected users
   */
  getConnectedUsers(): number[] {
    const userIds = new Set<number>();
    for (const client of this.connectedClients.values()) {
      if (client.userId) {
        userIds.add(client.userId);
      }
    }
    return Array.from(userIds);
  }
}
