import { kafkaClient } from "./kafkaClient";

async function initAdmin() {
  const admin = kafkaClient.admin();

  console.log("connecting admin");
  await admin.connect();
  console.log("connecting success");

  console.log("creating topic");
  await admin.createTopics({
    topics: [
      {
        topic: "orderUpdates",
        numPartitions: 2,
      },
    ],
  });

  console.log("topic created");

  admin.disconnect();
  console.log("disconnecting admin");
}

initAdmin();