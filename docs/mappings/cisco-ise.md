## Visual Map

```mermaid
graph TD
  callingStationID["ISE: Calling-Station-ID"]
  clientIP["Elastic: client.ip"]
  endpointID["ISE: Endpoint ID"]
  hostname["Elastic: host.hostname"]
  authStatus["ISE: Auth Status"]
  eventOutcome["Elastic: event.outcome"]
  authStatus --> eventOutcome
  callingStationID --> clientIP
  endpointID --> hostname