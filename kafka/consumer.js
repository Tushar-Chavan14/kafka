import { kafkaClient } from "./kafkaClient";

const groupId = process.argv[2]?.split("=")[1];

if (!groupId) {
  console.log("No group Id was provided");
  process.exit(1);
}

const orderConsumer = async () => {
  const consumer = kafkaClient.consumer({ groupId });

  console.log("Connecting to the Kafka consumer...");
  await consumer.connect();
  console.log("Connected to the Kafka consumer");

  console.log(`Subscribing to [orderUpdates] topic for group ${groupId}`);
  await consumer.subscribe({ topics: ["orderUpdates"], fromBeginning: true });
  console.log(`Subscribed to [orderUpdates] topic`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { orderId, product, amount, region } = JSON.parse(
        message.value.toString()
      );
        // perform heavy tasks or analytics
      console.log(
        `Received order update in group ${groupId} \n 
        from [${topic}] \n 
        (Partition: ${partition}) \n 
        Order ${orderId}: ${product} ($${amount}) ($${region})`
      );
    },
  });
};

orderConsumer();