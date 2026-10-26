import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import { ValidationPipe } from '@nestjs/common/pipes/validation.pipe.js';

async function bootstrap() {

  const app = await NestFactory.create(AppModule);

  app.enableCors({});

  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
    }),
  );

  const port = process.env.PORT ?? 3003;
  await app.listen(port);

  console.log(`🚀 Checkout Service running on port ${port}`);

}

await bootstrap();
