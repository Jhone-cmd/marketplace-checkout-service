import { Injectable, Logger } from '@nestjs/common';
import { RabbitmqService } from '../rabbitmq/rabbitmq.service.js';
import { PaymentOrderMessage } from '../payment-queue.interface.js';

@Injectable()
export class PaymentQueueService {
    private readonly logger = new Logger(PaymentQueueService.name);

    private readonly ROUTING_KEY = 'payment.order';
    private readonly EXCHANGE = 'payments';

    constructor(private readonly rabbitmqService: RabbitmqService) { }

    async publishPaymentOrder(paymentOrder: PaymentOrderMessage): Promise<void> {
        try {

            this.logger.log(
                `📤 Publishing payment order for orderId: ${paymentOrder.orderId}`,
            );

            const enrichmentMessage: PaymentOrderMessage = {
                ...paymentOrder,
                createdAt: paymentOrder.createdAt || new Date(),
                metadata: {
                    service: 'checkout-service',
                    timestamp: new Date().toISOString(),
                },
            };

            await this.rabbitmqService.publishMessage(this.EXCHANGE, this.ROUTING_KEY, enrichmentMessage);

            this.logger.log(
                `✅ Payment order published successfully: ` +
                `orderId=${paymentOrder.orderId}` +
                `amount=${paymentOrder.amount}` +
                `userId=${paymentOrder.userId}`,
            );

            this.logger.debug(
                `Payment order details: ${JSON.stringify(enrichmentMessage)}`,
            );


        } catch (error: Error | any) {
            this.logger.error(
                `❌ Failed to publish payment order: orderId=${paymentOrder.orderId}`,
                error.message || error,
            );
            throw error;
        }
    }

    private validatePaymentOrder(paymentOrder: PaymentOrderMessage): boolean {

        if (!paymentOrder.orderId) {
            this.logger.error('❌ Invalid payment order: missing orderId');
            return false;
        }

        if (!paymentOrder.userId) {
            this.logger.error('❌ Invalid payment order: missing userId');
            return false;
        }

        if (!paymentOrder.amount || paymentOrder.amount <= 0) {
            this.logger.error('❌ Invalid payment order: invalid amount');
            return false;
        }

        if (!paymentOrder.items || paymentOrder.items.length === 0) {
            this.logger.error('❌ Invalid payment order: no items');
            return false;
        }

        return true;
    }

    async publishPaymentOrderSafe(
        paymentOrder: PaymentOrderMessage,
    ): Promise<void> {

        if (!this.validatePaymentOrder(paymentOrder)) {
            this.logger.error('❌ Payment order validation failed, not publishing to queue');
            throw new Error('Invalid payment order');
        }

        this.logger.log(`📤 Publishing payment order for orderId: ${paymentOrder.orderId}`);
        await this.publishPaymentOrder(paymentOrder);
    }
}
