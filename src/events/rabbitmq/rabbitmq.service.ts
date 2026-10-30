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
                this.logger.error('❌  RabbitMQ connection error', error);
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
            this.logger.error('❌  Failed to disconnect from RabbitMQ', error.message || error);
        }
    }

    getChannel(): amqp.Channel {
        return this.channel;
    }

    getConnection(): amqp.ChannelModel {
        return this.connection;
    }
}
