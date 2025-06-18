## Visual Map

```mermaid
graph TD
  callingStationID["ISE: Calling-Station-ID"]
  clientIP["Elastic: client.ip"]
  endpointID["ISE: Endpoint ID"]
  hostname["Elastic: host.hostname"]

  callingStationID --> clientIP
  endpointID --> hostname
  event.code --> Event ID
  event.outcome --> Auth Status