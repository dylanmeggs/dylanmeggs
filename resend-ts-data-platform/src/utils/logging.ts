export function log(message: string, payload?: unknown) {
  console.log(message, payload ?? "");
}
