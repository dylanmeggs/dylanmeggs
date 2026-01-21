import { TrustSafetyEvent, EventType } from "./events";

export function isValidEvent(event: any): event is TrustSafetyEvent {
  return (
    typeof event?.event_id === "string" &&
    Object.values(EventType).includes(event?.event_type) &&
    typeof event?.user_id === "string" &&
    typeof event?.email_domain === "string" &&
    typeof event?.timestamp === "string"
  );
}
