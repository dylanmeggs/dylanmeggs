import { TrustSafetyEvent, EventType } from "../types/events";

type DomainMetric = {
  domain: string;
  window_start: string;
  event_count: number;
  spam_reports: number;
};

const WINDOW_MS = 60_000;
const state = new Map<string, DomainMetric>();

export function aggregate(event: TrustSafetyEvent): DomainMetric | null {
  const windowStart = new Date(
    Math.floor(Date.now() / WINDOW_MS) * WINDOW_MS
  ).toISOString();

  const key = `${event.email_domain}_${windowStart}`;

  const current = state.get(key) ?? {
    domain: event.email_domain,
    window_start: windowStart,
    event_count: 0,
    spam_reports: 0,
  };

  current.event_count += 1;

  if (event.event_type === EventType.SPAM_REPORT) {
    current.spam_reports += 1;
  }

  state.set(key, current);

  return current;
}
