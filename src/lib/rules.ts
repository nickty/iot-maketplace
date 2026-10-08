import { AlertRule } from "@prisma/client";

export interface TelemetryForRules {
  tenantId: string;
  deviceId: string;
  metric: string;
  value: number;
  timestamp: string;
}

/**
 * Evaluate a telemetry reading against a rule.
 * Returns true if the rule fires.
 */
export function evaluateRule(rule: AlertRule, reading: TelemetryForRules): boolean {
  // Rule must match the metric.
  if (rule.metric !== reading.metric) return false;

  // If the rule is device-specific, the device must match.
  if (rule.deviceId !== null && rule.deviceId !== reading.deviceId) return false;

  switch (rule.operator) {
    case "lt":  return reading.value <  rule.threshold;
    case "gt":  return reading.value >  rule.threshold;
    case "lte": return reading.value <= rule.threshold;
    case "gte": return reading.value >= rule.threshold;
    case "eq":  return reading.value === rule.threshold;
    case "neq": return reading.value !== rule.threshold;
    default:    return false;
  }
}

export function buildAlertMessage(rule: AlertRule, reading: TelemetryForRules) {
  return {
    ruleId: rule.id,
    tenantId: rule.tenantId,
    deviceId: reading.deviceId,
    metric: reading.metric,
    value: reading.value,
    threshold: rule.threshold,
    operator: rule.operator,
    severity: rule.severity,
    timestamp: reading.timestamp,
    firedAt: new Date().toISOString(),
  };
}