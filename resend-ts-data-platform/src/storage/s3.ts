import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { TrustSafetyEvent } from "../types/events";

const s3 = new S3Client({ region: "us-east-1" });

const RAW_BUCKET = "resend-trust-safety-raw-events";

export async function writeRawEventToS3(
  event: TrustSafetyEvent
): Promise<void> {
  const date = event.timestamp.slice(0, 10); // YYYY-MM-DD

  const key = [
    "events",
    `dt=${date}`,
    `${event.event_id}.json`
  ].join("/");

  const body = JSON.stringify(event);

  await s3.send(
    new PutObjectCommand({
      Bucket: RAW_BUCKET,
      Key: key,
      Body: body,
      ContentType: "application/json"
    })
  );
}
