# process-avro-event

## Introduction
An Azure Function designed to pick Confluent-Avro messages from an Event Hub Topic, convert these to JSON, and then send these to a Cosmos DB.

### Notes on Implementation
1. This solution was designed to work with 2 specific Confluent Avro message formats, defined by Temenos, in a customer project.  Your use of this project would necessitate getting the applicable schema file(s) for the messages you want to process.
2. You will need to add a local.settings.json file to this project, with the event hub and cosmos DB connection strings.  An example of this file is shown below

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "dotnet",
    "eventHubConnection": "Endpoint=...;EntityPath=xyzTopicName",
    "CosmosDBConnection": "AccountEndpoint=..."
  }
}
```
