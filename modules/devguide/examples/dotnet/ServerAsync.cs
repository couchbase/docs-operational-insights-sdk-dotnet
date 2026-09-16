// Couchbase Enterprise Analytics – Async Query Example (.NET)
//
// This sample demonstrates the full lifecycle of an asynchronous (server-side)
// analytics query using the Couchbase Analytics .NET SDK:
//
//   1. Connect to a Couchbase Analytics cluster.
//   2. Submit a long-running query via StartQueryAsync.
//   3. Poll the query status until results are ready.
//   4. Fetch and iterate over the result rows.
//   5. Inspect query metadata.
//   6. Discard results and dispose resources.
//
// Prerequisites:
//   - A running Couchbase cluster with the Analytics service enabled.
//   - Update the 'endpoint', 'username', and 'password' variables below.
//
// IMPORTANT: Port selection
//   The SDK defaults to port 80 (http) and 443 (https).
//   If connecting to Capella Columnar, the correct ports are typically
//   8095 (http) and 18095 (https).
//   Example: https://cb.xxxxxxxx.cloud.couchbase.com:18095

using System.Diagnostics;
using System.Text.Json;
using Couchbase.AnalyticsClient;
using Couchbase.AnalyticsClient.Async;
using Couchbase.AnalyticsClient.HTTP;
using Couchbase.AnalyticsClient.Options;
using Microsoft.Extensions.Logging;

// ──────────────────────────────────────────────
//  Configuration — update these for your cluster
// ──────────────────────────────────────────────

var endpoint = "http://localhost";
var username = "Administrator";
var password = "password";

// ──────────────────────────────────────────────
//  Setup logging
// ──────────────────────────────────────────────

// LoggerFactory implements IDisposable. The 'using' declaration ensures it is
// disposed at the end of the enclosing scope, flushing any buffered log output.
using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Debug)
        .AddSimpleConsole(options =>
        {
            options.IncludeScopes = true;
            options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
        });
});

// ──────────────────────────────────────────────
//  Connect to the cluster
// ──────────────────────────────────────────────

var credential = Credential.Create(username, password);

// Cluster implements IDisposable. The 'using' declaration ensures that the
// underlying HttpClient and related resources are released when we're done.
using var cluster = Cluster.Create(endpoint, credential, options => options
    .WithLogging(loggerFactory));

Console.WriteLine("Connected to cluster.");


// tag::server-async[]

// ──────────────────────────────────────────────
//  Start an async query
// ──────────────────────────────────────────────

var statement = """SELECT VALUE SLEEP("x", 100) FROM RANGE(1, 100) AS id;""";

var handle = await cluster.StartQueryAsync(statement, opts => opts
    .WithQueryTimeout(TimeSpan.FromSeconds(60)));

Console.WriteLine($"Query submitted. Handle: {handle}");

// ──────────────────────────────────────────────
//  Poll until results are ready
// ──────────────────────────────────────────────

var resultHandle = await WaitForQueryResultsAsync(handle, pollDelay: TimeSpan.FromSeconds(2.5), timeout: TimeSpan.FromSeconds(120));

Console.WriteLine($"Results ready. {resultHandle}");

// ──────────────────────────────────────────────
//  Fetch and display results
// ──────────────────────────────────────────────

// IQueryResult implements IAsyncDisposable. The 'await using' declaration
// ensures the underlying HTTP response stream is released after consumption.
// Forgetting this will leak connections.
await using var results = await resultHandle.FetchResultsAsync();

await foreach (var row in results.Rows)
{
    Console.WriteLine($"Found row: {row.ContentAs<JsonElement>()}");
}

Console.WriteLine($"Metadata: RequestId={results.MetaData.RequestId}, " +
                  $"ResultCount={results.MetaData.Metrics?.ResultCount}, " +
                  $"ElapsedTime={results.MetaData.Metrics?.ElapsedTime}");

// ──────────────────────────────────────────────
//  Discard results on the server
// ──────────────────────────────────────────────

await resultHandle.DiscardResultsAsync();
Console.WriteLine("Results discarded.");

// ──────────────────────────────────────────────
//  Helper: poll query status until ready
// ──────────────────────────────────────────────

/// <summary>
/// Polls the query handle at regular intervals until the server reports
/// that results are ready, or the timeout is reached.
/// </summary>
/// <param name="handle">The query handle returned by StartQueryAsync.</param>
/// <param name="pollDelay">Time between status polls.</param>
/// <param name="timeout">Maximum time to wait before throwing a TimeoutException.</param>
/// <returns>A <see cref="QueryResultHandle"/> that can be used to fetch results.</returns>
/// <exception cref="TimeoutException">Thrown when results are not ready within the timeout.</exception>
static async Task<QueryResultHandle> WaitForQueryResultsAsync(
    QueryHandle handle,
    TimeSpan pollDelay,
    TimeSpan timeout)
{
    var stopwatch = Stopwatch.StartNew();

    while (true)
    {
        try
        {
            var status = await handle.FetchStatusAsync();
            if (status.ResultsReady)
            {
                return status.ResultHandle();
            }

            Console.WriteLine($"Query status: {status}");
        }
        catch (Exception ex)
        {
            // Depending on the use case, you might want to break here or continue retrying.
            Console.WriteLine($"Error fetching query status: {ex.Message}");
        }

        if (stopwatch.Elapsed + pollDelay > timeout)
        {
            throw new TimeoutException($"Query results not ready within {timeout.TotalSeconds} seconds.");
        }

        Console.WriteLine($"Query results not ready yet, sleeping for {pollDelay.TotalSeconds}s...");
        await Task.Delay(pollDelay);
    }
}
// end::server-async[]

