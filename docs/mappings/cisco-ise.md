## Visual Map

```mermaid
graph TD
  callingStationID["ISE: Calling-Station-ID"]
  clientIP["Elastic: client.ip"]
  endpointID["ISE: Endpoint ID"]
  hostname["Elastic: host.hostname"]

  callingStationID --> clientIP
  endpointID --> hostname
  Event ID --> event.code
  Auth Status --> event.outcome