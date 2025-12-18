// src/api/websocket/websocket.module.ts

import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt';
import { WebSocketService } from './websocket.service';
import { WebSocketGateway } from './websocket.gateway';
import { UserModule } from '../user/user-module';

@Module({
  imports: [
    JwtModule.register({
      secret: process.env.JWT_SECRET || 'your_secret_key',
      signOptions: { expiresIn: '1h' },
    }),
    UserModule,
  ],
  providers: [WebSocketService, WebSocketGateway],
  exports: [WebSocketService, WebSocketGateway],
})
export class WebSocketModule {}
