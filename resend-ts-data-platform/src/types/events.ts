export enum EventType {
  EMAIL_SENT = "email_sent",
  EMAIL_BOUNCED = "email_bounced",
  SPAM_REPORT = "spam_report",
  ABUSE_FLAG = "abuse_flag",
}

export interface TrustSafetyEvent {
  event_id: string;
  event_type: EventType;
  user_id: string;
  ip_address: string;
  email_domain: string;
  timestamp: string;
  metadata?: Record<string, string>;
}
