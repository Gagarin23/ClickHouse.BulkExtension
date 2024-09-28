using System.Text.Json;
using ClickHouse.Client.ADO;
using ClickHouse.Client.BulkExtension.WebApiAggregator;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Models;


var builder = WebApplication.CreateBuilder(args);
var services = builder.Services;

services.Configure<AppOptions>(builder.Configuration.GetSection("AppOptions"));
services.AddSingleton<ClickHouseConnection>(s =>
{
    var options = s.GetRequiredService<IOptions<AppOptions>>();
    return new ClickHouseConnection(options.Value.ConnectionString);
});
services.AddSingleton<ChannelHolder>();
services.AddHostedService<BackgroundCopy>();
services.AddHostedService<BackgroundConsoleWriter>();

services.AddEndpointsApiExplorer();
services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapPost("/api/metrics", async (HttpContext context, ChannelHolder channelHolder) =>
    {
        var asyncIterator = JsonSerializer.DeserializeAsyncEnumerable<YourStructType>(context.Request.Body);
        await foreach (var item in asyncIterator)
        {
            await channelHolder.Writer.WriteAsync(item);
        }
    })
    .WithName("Metrics")
    .WithOpenApi(x =>
    {
        x.RequestBody = new OpenApiRequestBody
        {
            Content = new Dictionary<string, OpenApiMediaType>
            {
                ["application/json"] = new OpenApiMediaType
                {
                    Schema = new OpenApiSchema
                    {
                        Type = "array",
                        Items = new OpenApiSchema
                        {
                            Type = "object",
                            Properties = new Dictionary<string, OpenApiSchema>
                            {
                                ["Column1"] = new OpenApiSchema {Type = "string", Format = "date-time"},
                                ["Column2"] = new OpenApiSchema {Type = "number"},
                                ["Column3"] = new OpenApiSchema {Type = "number"}
                            }
                        }
                    }
                }
            }
        };

        return x;
    });

app.Run();