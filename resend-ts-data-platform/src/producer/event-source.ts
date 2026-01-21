import { TrustSafetyEvent } from "../types/events";

/**
 * Represents a connection to an upstream event source
 * (e.g. webhook receiver, internal event bus, or CDC stream).
 *
 * This implementation is illustrative and not executed.
 */
export class TrustSafetyEventSource {
  constructor(
    private readonly sourceName: string
  ) {}

  /**
   * In production, this would:
   * - Poll an upstream service
   * - Or receive events via webhook
   * - Or subscribe to an internal bus
   */
  async fetchEvents(): Promise<TrustSafetyEvent[]> {
    // Illustrative static payload to demonstrate shape
    return [
      {
        event_id: "evt_123",
        event_type: "spam_report",
        user_id: "user_42",
        ip_address: "203.0.113.10",
        email_domain: "spammy.io",
        timestamp: "2026-01-21T18:42:00Z",
        metadata: {
          campaign_id: "cmp_77",
          provider: "ses"
        }
      }
    ];
  }
}
