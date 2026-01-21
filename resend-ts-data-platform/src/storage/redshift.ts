export async function writeMetric(metric: unknown) {
  // In production, this would batch insert into Redshift
  console.log("Writing metric", metric);
}
