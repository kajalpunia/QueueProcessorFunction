using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace ProcessQueueMessage
    {
        public class ProcessQueueMessage
        {
            private readonly ILogger<ProcessQueueMessage> _logger;
            private static readonly HttpClient _httpClient = new HttpClient();
            private static readonly string apiUrl = "https://eventprocessor.azure-api.net/processorApi/api/events/process";
            private static readonly string subscriptionKey = "9ed99a8f65e44641897951ccb0aeb4e3";

            public ProcessQueueMessage(ILogger<ProcessQueueMessage> logger)
            {
                _logger = logger;
            }

            [Function(nameof(ProcessQueueMessage))]
            public async Task Run(
                [ServiceBusTrigger("InputQueue", Connection = "ConnectionString")] ServiceBusReceivedMessage message,
                ServiceBusMessageActions messageActions)
            {
                _logger.LogInformation("Received Message ID: {id}", message.MessageId);
                _logger.LogInformation("Message Body: {body}", message.Body);

                bool isDuplicate = await IsDuplicateAsync(message.MessageId);
                if (isDuplicate)
                {
                    _logger.LogInformation("Duplicate message detected. Skipping message processing.");
                    await messageActions.CompleteMessageAsync(message);
                    return;
                }

                var success = await ProcessMessageAsync(message.Body.ToString());
                if (success)
                {
                    _logger.LogInformation("Message successfully processed and sent to API.");
                    await MarkAsProcessedAsync(message.MessageId); 
                }
                else
                {
                    _logger.LogError("Failed to send message to API.");
                }

                await messageActions.CompleteMessageAsync(message);
            }

            private async Task<bool> IsDuplicateAsync(string messageId)
            {
                return false; 
            }

            private async Task<bool> ProcessMessageAsync(string messageBody)
            {
                try
                {
                    var payload = new
                    {
                        eventId = Guid.NewGuid().ToString(), 
                        eventData = messageBody 
                    };

                    var jsonPayload = JsonConvert.SerializeObject(payload);

                    var requestData = new StringContent(jsonPayload, Encoding.UTF8, "application/json");

                    requestData.Headers.Add("Ocp-Apim-Subscription-Key", subscriptionKey);

                    var response = await _httpClient.PostAsync(apiUrl, requestData);

                    return response.IsSuccessStatusCode;
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error while processing message: {ex.Message}");
                    return false;
                }
            }

            private async Task MarkAsProcessedAsync(string messageId)
            {
                _logger.LogInformation($"Message ID: {messageId} marked as processed.");
                await Task.CompletedTask;
            }
        }
    } 
