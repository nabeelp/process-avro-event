using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Avro.IO;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace ProcessAvroEvent
{
    public static class ProcessAvroEvent
    {
        [FunctionName("ProcessAvroEvent")]
        public static async Task Run(
            [EventHubTrigger("table-update", Connection = "eventHubConnection")] EventData[] events, 
            [CosmosDB(
                databaseName: "Temenos",
                collectionName: "Events",
                ConnectionStringSetting = "CosmosDBConnection")]
                IAsyncCollector<JObject> eventsOut,
            ILogger log)
        {
            log.LogInformation($"ProcessAvroEvent triggered with {events.Count()} events");

            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    // convert messageBody of this event to a stream
                    MemoryStream stream = new MemoryStream(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // skip the first 2 bytes
                    stream.Position = 3;

                    // decode the stream and get the schema number
                    BinaryDecoder decoder = new BinaryDecoder(stream);
                    var magicCode = (decoder.ReadBoolean() == false);
                    var schemaNumber = decoder.ReadLong();

                    // get the appropriate schema
                    Schema schema = null;
                    switch (schemaNumber)
                    {
                        case 23:
                            schema = Schema.Parse(File.ReadAllText(@"SerializationID-46-CUSTOMER.avsc"));
                            break;
                        case -21:
                            schema = Schema.Parse(File.ReadAllText(@"SerializationID-41-DE_ADDRESS.avsc"));
                            break;
                        default:
                            throw new Exception("Unknown schema nuumber: " + schemaNumber);
                    }

                    // read the avro message using the identified schema            
                    var reader = new DefaultReader(schema, schema);
                    GenericRecord record = reader.Read(null, schema, schema, decoder) as GenericRecord;

                    // convert to JSON and return
                    JObject outputData = ConvertToJson(record);
                    eventsOut.AddAsync(outputData).Wait();

                    // Replace these two lines with your processing logic.
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // TODO: consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        private static JObject ConvertToJson(GenericRecord record)
        {
            JObject myObject = new JObject();
            foreach (var field in record.Schema.Fields)
            {
                if (record.TryGetValue(field.Name, out object value))
                {
                    if (value == null)
                    {
                        myObject[field.Name] = null;
                    }
                    else if (value.GetType().Name == "String")
                    {
                        myObject[field.Name] = Convert.ToString(value);
                    }
                    else if (value.GetType().Name == "Dictionary`2")
                    {
                        var entries = (value as Dictionary<string, object>).Select(d =>
                            string.Format("\"{0}\": \"{1}\"", d.Key, string.Join(",", d.Value.ToString())));
                        myObject[field.Name] = JObject.Parse("{" + string.Join(",", entries) + "}");
                    }
                    else
                    {
                        switch (field.Schema.Tag)
                        {
                            case Schema.Type.Null:
                                myObject[field.Name] = null;
                                break;
                            case Schema.Type.Boolean:
                                myObject[field.Name] = Convert.ToBoolean(value);
                                break;
                            case Schema.Type.Int:
                            case Schema.Type.Long:
                                myObject[field.Name] = Convert.ToInt64(value);
                                break;
                            case Schema.Type.Float:
                            case Schema.Type.Double:
                                myObject[field.Name] = Convert.ToDouble(value);
                                break;
                            case Schema.Type.Record:
                                myObject[field.Name] = ConvertToJson(value as GenericRecord);
                                break;
                            case Schema.Type.Array:
                                JArray thisArray = new JArray();
                                foreach (var item in (value as Array))
                                {
                                    thisArray.Add(ConvertToJson(item as GenericRecord));
                                }
                                myObject[field.Name] = thisArray;
                                break;
                            default:
                                myObject[field.Name] = Convert.ToString(value);
                                break;
                        }
                    }
                }
            }
            return myObject;
        }
    }
}
