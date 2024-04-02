using System.Collections.Generic;
using System.Net;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Parquet;

namespace FunctionHelper;

public class ParquetAPI
{
    private readonly ILogger _logger;

    public ParquetAPI(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<ParquetAPI>();
    }

    [Function("ParquetAPI")]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post" ,Route = "parquet/count")] HttpRequestData req,
        string fileName,
        FunctionContext executionContext)
    {
        var dto = await req.ReadFromJsonAsync<Dto>();
        
        string connectionString = Environment.GetEnvironmentVariable("BlobStorageConnectionString");
        string containerName = dto.Container;
        string blobName = dto.FileName;

        BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        BlobClient blobClient = containerClient.GetBlobClient(blobName);

        try
        {
            using (var memoryStream = new MemoryStream())
            {
                await blobClient.DownloadToAsync(memoryStream);
                memoryStream.Seek(0, SeekOrigin.Begin);

                using (var parquetReader = ParquetReader.ReadTableFromStreamAsync(memoryStream))
                {
                    // Read the file schema
                    var result = await parquetReader;
                    var response = req.CreateResponse(HttpStatusCode.OK);
                    await response.WriteStringAsync(result.Count.ToString());
                    return response;
                }
            }
        }
        catch (Exception ex)
        {
            var errorResponse = req.CreateResponse(HttpStatusCode.InternalServerError);
            await errorResponse.WriteStringAsync("Oops, something went wrong");
            return errorResponse;
        }
    }
}

public class Dto
{
    public string FileName { get; set; }
    public string Container { get; set; }
}