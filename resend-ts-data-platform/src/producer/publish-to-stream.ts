import { KinesisClient, PutRecordCommand } from "@aws-sdk/client-kinesis";
import { TrustSafetyEventSource } from "./event-source";

const client = new KinesisClient({ region: "us-east-1" });
const STREAM_NAME = "resend-trust-safety-events";

export async function publishEvents() {
  const source = new TrustSafetyEventSource("email-events-service");

  const events = await source.fetchEvents();

  for (const event of events) {
    await client.send(
      new PutRecordCommand({
        StreamName: STREAM_NAME,
        PartitionKey: event.user_id,
        Data: Buffer.from(JSON.stringify(event))
      })
    );
  }
}
