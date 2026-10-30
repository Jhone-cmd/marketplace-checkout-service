import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as amqp from 'amqplib';

@Injectable()
export class RabbitmqService implements OnModuleInit, OnModuleDestroy {
    private readonly logger = new Logger(RabbitmqService.name);

    private connection: amqp.ChannelModel;
    private channel: amqp.Channel;

    constructor(private readonly configService: ConfigService) { }

    async onModuleInit() {
        await this.connect();
    }

    async onModuleDestroy() {
        await this.disconnect();
    }


    private async connect() {
        try {
            this.connection = await amqp.connect(this.configService.get<string>('RABBITMQ_URL') || 'amqp://admin:admin@localhost:5672');
            this.channel = await this.connection.createChannel();
            this.logger.log('✅ Connected to RabbitMQ sucessfully');

            // Events listener para monitorar a conexão
            this.connection.on('error', (error) => {
                this.logger.error('❌ RabbitMQ connection error', error);
            });

            this.connection.on('close', () => {
                this.logger.warn('⚠️  RabbitMQ connection closed');
            });

            this.connection.on('blocked', (reason) => {
                this.logger.warn('⚠️  RabbitMQ connection blocked', reason);
            });

            this.connection.on('unblocked', () => {
                this.logger.log('✅ RabbitMQ connection unblocked');
            });
        } catch (error: Error | any) {
            this.logger.warn('⚠️  Failed to connect to RabbitMQ', error.message || error);
            throw error;
        }
    }

    private async disconnect() {
        try {
            if (this.channel) {
                await this.channel.close();
                this.logger.log('✅ RabbitMQ channel closed');
            }
            if (this.connection) {
                await this.connection.close();
                this.logger.log('✅ Disconnected from RabbitMQ successfully');
            }

        } catch (error: Error | any) {
            this.logger.error('❌ Failed to disconnect from RabbitMQ', error.message || error);
        }
    }

    getChannel(): amqp.Channel {
        return this.channel;
    }

    getConnection(): amqp.ChannelModel {
        return this.connection;
    }

    async publishMessage(exchange: string, routingKey: string, message: any): Promise<void> {
        try {
            if (!this.channel) {
                this.logger.warn('⚠️  RabbitMQ channel not available, skipping message publish');
                return;
            }

            await this.channel.assertExchange(exchange, 'topic', { durable: true });
            const messageBuffer = Buffer.from(JSON.stringify(message));

            const published = this.channel.publish(exchange, routingKey, messageBuffer, {
                persistent: true,
                timestamp: Date.now(),
                contentType: 'application/json',
            });

            if (!published) {
                throw new Error('❌ Failed to publish message to RabbitMQ');
            }

            this.logger.log(`✅ Message published to"${exchange}":"${routingKey}"`);
            this.logger.debug(`Message content: ${JSON.stringify(message)}`);


        } catch (error: Error | any) {
            this.logger.error('❌ Failed to publish message', error.message || error);
        }
    }

    async subscribeToQueue(queueName: string, exchange: string, routingKey: string, cb: (message: unknown) => Promise<void>): Promise<void> {
        try {
            if (!this.channel) {
                this.logger.warn('⚠️  RabbitMQ channel not available, skipping queue subscription');
                return;
            }
            await this.channel.assertExchange(exchange, 'topic', { durable: true });

            const queue = await this.channel.assertQueue(queueName, { durable: true, arguments: { 'x-message-ttl': 86400000, 'x-max-length': 10000 } });

            await this.channel.bindQueue(queue.queue, exchange, routingKey);

            await this.channel.prefetch(1); // Process one message at a time

            this.channel.consume(queue.queue, async (msg) => {
                if (msg) {
                    try {
                        const message: unknown = JSON.parse(msg.content.toString());

                        this.logger.log(`📨 Message received from queue: ${queueName}`);
                        this.logger.debug(`Message content: ${JSON.stringify(message)}`);

                        await cb(message);

                        this.channel.ack(msg);

                        this.logger.log(
                            `✅ Message processed successfully from queue: ${queueName}`,
                        );

                    } catch (error: Error | any) {
                        this.logger.error('❌ Error processing message', error.message || error);
                        this.channel.nack(msg, false, false); // Reject the message and do not requeue
                        // TODO: dead letter queue handling can be implemented here if needed
                    }
                }
            });
        } catch (error: Error | any) {
            this.logger.error(`❌ Error subscribing to queue ${queueName}`, error.message || error);
        }
    }
}
