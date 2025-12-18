#!/bin/bash

# Enhanced deployment script with dependency installation
echo "🚀 Deploying Trading Bot with WebSocket Dependencies to AWS..."

# Build the project first
echo "📦 Building TypeScript project..."
npm run build

# Sync files to AWS
echo "📤 Syncing files to AWS..."
rsync -avz --progress \
  --exclude 'node_modules' \
  --exclude '.git' \
  --exclude '.env' \
  --exclude 'dist' \
  --exclude '*.log' \
  --exclude '.DS_Store' \
  --exclude 'coverage' \
  --exclude 'test' \
  --exclude '*.md' \
  --exclude 'websocket-client-example.html' \
  --exclude 'test-*.js' \
  -e "ssh -i ~/.ssh/trading.pem" \
  . ubuntu@54.152.108.69:~/app

echo "✅ Files synced successfully!"

echo "🔧 Installing dependencies on server..."
ssh -i ~/.ssh/trading.pem ubuntu@54.152.108.69 << 'EOF'
cd ~/app
echo "📦 Installing WebSocket dependencies..."
npm install @nestjs/websockets @nestjs/platform-socket.io socket.io --legacy-peer-deps
echo "🔨 Building project on server..."
npm run build
echo "🔄 Restarting PM2 process..."
pm2 restart trading-bot
echo "✅ Deployment complete!"
EOF

echo "🎉 Deployment finished! Check server logs with: pm2 logs trading-bot"
