## Visual Map

```mermaid
graph TD
  subgraph ISE Logs
    callingStationID["ISE: Calling-Station-ID"]
    endpointID["ISE: Endpoint ID"]
    authStatus["ISE: Auth Status"]
  end

  subgraph Elasticsearch
    clientIP["Elastic: client.ip"]
    hostname["Elastic: host.hostname"]
    eventOutcome["Elastic: event.outcome"]
  end

  callingStationID --> clientIP
  endpointID --> hostname
  authStatus --> eventOutcome