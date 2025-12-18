// src/subscription/subscription.service.ts

import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { JsonRpcProvider, Contract, formatUnits } from "ethers";
import { Cron, CronExpression } from "@nestjs/schedule";
import { UserRepository } from "../user/user-repository";
import { EmailService } from "../email/email-service";
import { User } from "../user/user-entity";

@Injectable()
export class SubscriptionService implements OnModuleInit {
  private readonly logger = new Logger(SubscriptionService.name);
  private provider: JsonRpcProvider;
  private walletAddress: string;
  private ascContractAddress: string;
  private ascABI: string[];

  // Uniswap V3 Subgraph endpoint
  private uniswapV3Endpoint =
    "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3";
  // The V3 pool for ASC–USDT on mainnet
  private ascUsdtPoolAddress = "0x9DF8f2c89E04C25B6c3636E718dd62d5D16230d9";
  private uniswapV3PoolABI = [
    "function slot0() external view returns (uint160, int24, uint16, uint16, uint16, uint8, bool)",
    "function token0() external view returns (address)",
    "function token1() external view returns (address)",
  ];

  constructor(
    private readonly userRepository: UserRepository,
    private readonly emailService: EmailService,
  ) {
    // Initialize Ethereum mainnet provider
    this.provider = new JsonRpcProvider(
      process.env.ETHEREUM_RPC_URL ||
        "https://mainnet.infura.io/v3/9de0180f470d430485e9963b80d203f6",
    );

    // Wallet + ASC addresses
    this.walletAddress =
      process.env.ASC_WALLET_ADDRESS ||
      "0x121fec926152A209e8f20f78d875335402C1bA98";
    this.ascContractAddress =
      process.env.ASC_CONTRACT_ADDRESS ||
      "0x2B00F09C8958622a89A29Fb23fAA4e405Dfd9bB6";

    // Standard ERC-20 Transfer event ABI
    this.ascABI = [
      "event Transfer(address indexed from, address indexed to, uint256 value)",
    ];
  }

  // ========== 1) Listen for ASC transfers on-chain ==========
  async startListening(): Promise<void> {
    try {
      this.logger.log("SubscriptionService: Starting to listen for ASC transfers...");

      // Verify provider connection
      await this.provider.getNetwork();
      this.logger.log("Provider connection verified");

      const contract = new Contract(
        this.ascContractAddress,
        this.ascABI,
        this.provider
      );

      // Add error handling for the contract
      contract.on("error", (error) => {
        this.logger.error(`Contract event error: ${error.message}`);
        // Attempt to reconnect
        this.restartListening();
      });

      contract.on("Transfer", async (from: string, to: string, value: bigint) => {
        try {
          // Check if ASC was sent to our designated wallet
          if (to.toLowerCase() === this.walletAddress.toLowerCase()) {
            const ascAmount = parseFloat(formatUnits(value, 18));
            this.logger.log(
              `Received ASC transfer from ${from}. Amount: ${ascAmount}`,
            );

            const amountInUSD = await this.getAscValueInUSD(ascAmount);
            this.logger.log(`Equivalent USD (approx): $${amountInUSD.toFixed(2)}`);

            await this.handlePartialPayment(from, amountInUSD);
          }
        } catch (error) {
          this.logger.error(`Error processing transfer event: ${error instanceof Error ? error.message : String(error)}`);
        }
      });

      this.logger.log(`Listening for transfers to wallet: ${this.walletAddress}`);
    } catch (error) {
      this.logger.error(`Failed to start listening: ${error instanceof Error ? error.message : String(error)}`);
      // Attempt to restart after delay
      setTimeout(() => this.restartListening(), 30000);
    }
  }

  private async restartListening(): Promise<void> {
    this.logger.log("Attempting to restart transfer listening...");
    try {
      // Reinitialize provider
      this.provider = new JsonRpcProvider(
        process.env.ETHEREUM_RPC_URL ||
          "https://mainnet.infura.io/v3/9de0180f470d430485e9963b80d203f6"
      );
      await this.startListening();
    } catch (error) {
      this.logger.error(`Failed to restart listening: ${error instanceof Error ? error.message : String(error)}`);
      // Try again after delay
      setTimeout(() => this.restartListening(), 30000);
    }
  }

  /**
   * handlePartialPayment:
   *  - Finds the user by wallet
   *  - Increments partial_usd_balance
   *  - If partial_usd_balance >= $10, grant subscription
   *  - Otherwise, send email about how much is missing
   */
  private async handlePartialPayment(
    senderAddress: string,
    amountInUSD: number,
  ): Promise<void> {
    const requiredUsdAmount = 30; // $10 for subscription

    try {
      const user = await this.userRepository.findUserByWallet(senderAddress);
      if (!user) {
        this.logger.warn(
          `No user found with wallet ${senderAddress}. Ignoring payment.`,
        );
        return;
      }

      // Check if user has an active subscription
      if (
        user.has_subscription &&
        user.subscription_expiry &&
        user.subscription_expiry > new Date()
      ) {
        this.logger.log(
          `User ${user.email} is already subscribed. Payment ignored.`,
        );
        return;
      }

      // Accumulate partial balance
      const oldBalance = Number(user.partial_usd_balance) || 0;
      const newBalance = oldBalance + amountInUSD;
      this.logger.log(
        `User ${user.email} partial balance old: $${oldBalance}, adding: $${amountInUSD}, new: $${newBalance}`,
      );

      if (newBalance >= requiredUsdAmount) {
        // Sufficient credit to renew subscription
        const remainingUsd = newBalance - requiredUsdAmount;
        user.partial_usd_balance = remainingUsd;
        user.has_subscription = true;
        user.subscription_expiry = this.calculateNextExpiry();

        await this.userRepository.save(user);

        // Grant subscription
        await this.grantSubscription(user);

        // Notify user about remaining credit
        if (remainingUsd > 0) {
          const remainingAsc = await this.getAscAmountFromUsd(remainingUsd);
          await this.emailService.sendCreditRemainingEmail(
            user.email,
            remainingUsd,
            remainingAsc,
          );
        }
      } else {
        // Insufficient credit, store as partial payment and notify user
        user.partial_usd_balance = newBalance;
        await this.userRepository.save(user);

        const missingUsd = requiredUsdAmount - newBalance;
        const missingAsc = await this.getAscAmountFromUsd(missingUsd);

        await this.emailService.sendPartialPaymentEmail(
          user.email,
          newBalance,
          missingUsd,
          missingAsc,
        );
      }
    } catch (err) {
      this.logger.error("Error handling partial payment:", err);
    }
  }

  /**
   * Helper method to calculate next subscription expiry date (1 month from now)
   */
  private calculateNextExpiry(): Date {
    const expiry = new Date();
    expiry.setMonth(expiry.getMonth() + 1);
    return expiry;
  }

  /**
   * Converts USD amount to ASC tokens
   * @param usdAmount USD amount
   */
  private async getAscAmountFromUsd(usdAmount: number): Promise<number> {
    try {
      const priceAscInUsd = await this.getAscValueInUSD(1);
      if (priceAscInUsd === 0) {
        throw new Error("ASC price in USD is zero.");
      }
      const ascAmount = usdAmount / priceAscInUsd;
      return ascAmount;
    } catch (error) {
      this.logger.error("Error calculating ASC amount from USD:", error);
      return 0;
    }
  }

  // ========== 2) Get ASC Price in USD from Uniswap V3 ==========
  public async getAscValueInUSD(ascAmount: number): Promise<number> {
    try {
      const poolContract = new Contract(
        this.ascUsdtPoolAddress,
        this.uniswapV3PoolABI,
        this.provider,
      );
      const token0 = (await poolContract.token0()).toLowerCase();
      const token1 = (await poolContract.token1()).toLowerCase();

      const decimalsAsc = 18;
      const decimalsUsdt = 6;

      const slot0 = await poolContract.slot0();
      const sqrtPriceX96 = slot0[0];

      // Convert BigInt
      const sqrtPrice = BigInt(sqrtPriceX96.toString());
      const Q96 = 1n << 96n; // 2^96
      const Q192 = 1n << 192n; // 2^192

      const numerator = sqrtPrice * sqrtPrice; // sqrtPrice^2
      const denominator = Q192;
      let ratio = Number(numerator) / Number(denominator);

      const decimalsFactor = 10 ** (decimalsAsc - decimalsUsdt);
      let priceAscInUsdt = 0;

      if (token0 === this.ascContractAddress.toLowerCase()) {
        // ASC = token0
        priceAscInUsdt = ratio * decimalsFactor;
      } else {
        // ASC = token1 => invert
        const inverseRatio = 1 / ratio;
        const decimalsFactor2 = 10 ** (decimalsUsdt - decimalsAsc);
        priceAscInUsdt = inverseRatio * decimalsFactor2;
      }

      const totalInUsdt = ascAmount * priceAscInUsdt;
      return totalInUsdt;
    } catch (error) {
      this.logger.error("Error reading slot0 from Uniswap V3 pool:", error);
      return 0;
    }
  }

  // ========== 3) Grant Subscription if Payment is Valid ==========
  public async grantSubscription(user: User): Promise<void> {
    try {
      // Set subscription expiry date to 1 month from now
      const subscriptionExpiry = this.calculateNextExpiry();

      // Update subscription status and expiry in DB
      user.has_subscription = true;
      user.subscription_expiry = subscriptionExpiry;
      await this.userRepository.save(user);
      this.logger.log(
        `Subscription granted to user: ${user.email}, expires on: ${subscriptionExpiry}`,
      );

      // Send subscription confirmation email
      await this.emailService.sendSubscriptionRenewedEmail(
        user.email,
        subscriptionExpiry,
        0,
        0,
      );
    } catch (error) {
      this.logger.error(
        "Error granting subscription:",
        (error as Error).message,
      );
    }
  }

  // ========== 4) Scheduled Task to Handle Pre-Expiry Notifications ==========
  @Cron(CronExpression.EVERY_DAY_AT_MIDNIGHT)
  async handlePreExpiryNotifications(): Promise<void> {
    this.logger.log("Running pre-expiry notification check.");

    try {
      const targetDate = new Date();
      targetDate.setDate(targetDate.getDate() + 3); // 3 days from now
      targetDate.setHours(0, 0, 0, 0); // Start of the day

      // Find users whose subscription expires exactly 3 days from now
      const preExpiryUsers =
        await this.userRepository.findSubscriptionsExpiringOn(targetDate);

      for (const user of preExpiryUsers) {
        this.logger.log(
          `Sending pre-expiry notification to user: ${user.email}`,
        );

        // Calculate how much ASC is needed to renew ($10 USD worth)
        const requiredUsdAmount = 30;
        const requiredAsc = await this.getAscAmountFromUsd(requiredUsdAmount);

        // Prepare email details
        const currentCreditUsd = Number(user.partial_usd_balance) || 0;
        const currentCreditAsc =
          await this.getAscAmountFromUsd(currentCreditUsd);

        // **Runtime Check:** Ensure subscription_expiry is not null
        if (!user.subscription_expiry) {
          this.logger.warn(
            `User ${user.email} has no subscription expiry date. Skipping email.`,
          );
          continue; // Skip sending email for this user
        }

        // Send pre-expiry notification email
        await this.emailService.sendPreExpiryEmail(
          user.email,
          user.subscription_expiry, // Type: Date | null -> Date (guaranteed not null)
          currentCreditUsd,
          currentCreditAsc,
          requiredUsdAmount,
          requiredAsc,
        );
      }
    } catch (error) {
      this.logger.error("Error during pre-expiry notification check:", error);
    }
  }

  // ========== 5) Scheduled Task to Handle Subscription Renewals ==========
  @Cron(CronExpression.EVERY_DAY_AT_1AM)
  async handleSubscriptionRenewals(): Promise<void> {
    this.logger.log("Running subscription renewal check.");

    try {
      const targetDate = new Date();
      targetDate.setHours(0, 0, 0, 0); // Today

      // Find users whose subscription expired on or before today
      const expiredUsers =
        await this.userRepository.findSubscriptionsExpiredOnOrBefore(
          targetDate,
        );

      for (const user of expiredUsers) {
        this.logger.log(
          `Processing renewal for expired subscription of user: ${user.email}`,
        );

        const requiredUsdAmount = 10;
        const availableCredit = Number(user.partial_usd_balance) || 0;

        if (availableCredit >= requiredUsdAmount) {
          // Deduct $10 USD worth of ASC from credits
          const remainingUsd = availableCredit - requiredUsdAmount;
          user.partial_usd_balance = remainingUsd;
          user.has_subscription = true;
          user.subscription_expiry = this.calculateNextExpiry();

          await this.userRepository.save(user);

          // **Runtime Check:** Ensure subscription_expiry is not null
          if (!user.subscription_expiry) {
            this.logger.warn(
              `User ${user.email} has no subscription expiry date after renewal. Skipping email.`,
            );
            continue; // Skip sending email for this user
          }

          // Send renewal confirmation email
          await this.emailService.sendSubscriptionRenewedEmail(
            user.email,
            user.subscription_expiry, // Type: Date | null -> Date (guaranteed not null)
            remainingUsd,
            await this.getAscAmountFromUsd(remainingUsd),
          );

          this.logger.log(`Subscription renewed for user: ${user.email}`);
        } else {
          // Insufficient credits, mark subscription as inactive
          user.has_subscription = false;
          await this.userRepository.save(user);

          const missingUsd = requiredUsdAmount - availableCredit;
          const missingAsc = await this.getAscAmountFromUsd(missingUsd);

          // Send subscription deactivation email
          await this.emailService.sendSubscriptionDeactivatedEmail(
            user.email,
            missingUsd,
            missingAsc,
          );

          this.logger.log(
            `Subscription deactivated for user: ${user.email} due to insufficient credits.`,
          );
        }
      }
    } catch (error) {
      this.logger.error("Error during subscription renewal check:", error);
    }
  }

  @Cron(CronExpression.EVERY_MINUTE)
  async deactivateExpiredSubscriptions(): Promise<void> {
    try {
      this.logger.log('Checking for expired subscriptions...');
      const expiredUsers = await this.userRepository.findExpiredSubscriptions();

      if (expiredUsers.length > 0) {
        this.logger.log(`Found ${expiredUsers.length} expired subscriptions`);
        
        for (const user of expiredUsers) {
          user.has_subscription = false;
          await this.userRepository.save(user);
          this.logger.log(`Deactivated subscription for user ${user.email}`);
        }
      }
    } catch (error) {
      this.logger.error('Error deactivating expired subscriptions:', error);
    }
  }

  public async isSubscriptionActive(userId: number): Promise<boolean> {
    try {
      const user = await this.userRepository.findOne({ where: { id: userId } });
      if (!user) {
        throw new Error('User not found');
      }

      const now = new Date();
      const expiryDate = user.subscription_expiry ? new Date(user.subscription_expiry) : null;
      
      // Log for debugging
      this.logger.debug(`Checking subscription for user ${userId}:`);
      this.logger.debug(`Current time: ${now.toISOString()}`);
      this.logger.debug(`Expiry date: ${expiryDate?.toISOString()}`);
      
      const isExpired = expiryDate && now > expiryDate;

      if (isExpired && user.has_subscription) {
        // Update subscription status
        user.has_subscription = false;
        await this.userRepository.save(user);
        this.logger.log(`Subscription marked as inactive for user: ${user.email}`);
        return false;
      }

      return user.has_subscription && !isExpired;
    } catch (error) {
      this.logger.error(`Error checking subscription status: ${error}`);
      return false;
    }
  }

  async onModuleInit() {
    await this.startListening();
    this.logger.log('Subscription service initialized');
  }
}
