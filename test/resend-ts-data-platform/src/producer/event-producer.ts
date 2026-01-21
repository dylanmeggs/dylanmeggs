import { KinesisClient, PutRecordCommand } from "@aws-sdk/client-kinesis";
import { v4 as uuidv4 } from "uuid";
import { EventType, TrustSafetyEvent } from "../types/events";

const client = new KinesisClient({ region: "us-east-1" });
const STREAM_NAME = "resend-trust-safety-events";

function randomEvent(): TrustSafetyEvent {
  return {
    event_id: uuidv4(),
    event_type: Object.values(EventType)[
      Math.floor(Math.random() * 4)
    ],
    user_id: `user_${Math.floor(Math.random() * 1000)}`,
    ip_address: "192.168.1.1",
    email_domain: ["gmail.com", "yahoo.com", "spammy.io"][
      Math.floor(Math.random() * 3)
    ],
    timestamp: new Date().toISOString(),
    metadata: {
      campaign_id: `cmp_${Math.floor(Math.random() * 100)}`,
    },
  };
}

async function sendEvent() {
  const event = randomEvent();

  await client.send(
    new PutRecordCommand({
      StreamName: STREAM_NAME,
      PartitionKey: event.user_id,
      Data: Buffer.from(JSON.stringify(event)),
    })
  );

  console.log("Sent event", event.event_type);
}

setInterval(sendEvent, 200);
