import { Kafka } from "kafkajs";

export const kafkaClient = new Kafka({
  clientId: "myKafka-app",
  brokers: ["127.0.0.1:9092"],
});