export function currentMinute(): string {
  return new Date().toISOString().slice(0, 16);
}
