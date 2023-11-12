import { kafkaClient } from "./kafkaClient";
import readline from "readline";

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const orderProducer = async () => {
  const producer = kafkaClient.producer();

  console.log("Connecting to the Kafka producer...");
  await producer.connect();
  console.log("Connected to the Kafka producer");

  rl.setPrompt("> Enter order information (e.g., Order123 ProductA $50 IN or US)\n");
  rl.prompt();

  rl.on("line", async (line) => {
    const [orderId, product, amount, region] = line.split(" ");

    if (
      !orderId ||
      !product ||
      !amount ||
      isNaN(parseFloat(amount) || !region)
    ) {
      console.log("Invalid input. Please enter valid order information.");
      rl.prompt();
      return;
    }

    await producer.send({
      topic: "orderUpdates",
      messages: [
        {
          key: orderId,
          partition: region === "IN" ? 0 : 1,
          value: JSON.stringify({
            orderId,
            product,
            amount: parseFloat(amount),
            region,
          }),
        },
      ],
    });

    console.log(
      `Order update sent to Kafka topic [orderUpdates] for order ${orderId}.`
    );

    rl.prompt();
  }).on("close", async () => {
    await producer.disconnect();
    console.log("Disconnected from the Kafka producer");
    process.exit(0);
  });
};

orderProducer();