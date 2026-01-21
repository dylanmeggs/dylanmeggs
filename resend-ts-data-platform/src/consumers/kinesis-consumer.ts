import { KinesisStreamEvent } from "aws-lambda";
import { isValidEvent } from "../types/validation";
import { writeRawEventToS3 } from "../storage/s3";
import { aggregate } from "./aggregations";

export const handler = async (event: KinesisStreamEvent) => {
  for (const record of event.Records) {
    const payload = Buffer.from(
      record.kinesis.data,
      "base64"
    ).toString("utf-8");

    const parsed = JSON.parse(payload);

    if (!isValidEvent(parsed)) {
      console.error("Invalid event", parsed);
      continue;
    }

    // 1. Store immutable raw data
    await writeRawEventToS3(parsed);

    // 2. Derive metrics
    const metric = aggregate(parsed);

    if (metric) {
      console.log("Derived metric", metric);
      // writeMetric(metric) – intentionally stubbed
    }
  }
};
