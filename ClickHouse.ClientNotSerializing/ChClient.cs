namespace ClickHouse.ClientNotSerializing;

using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Readers;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

public class ChClient(IChSessionn chSession,
    ILogger<ChClient> logger)
{
    private readonly IChSessionn _chSession = chSession;
    private readonly ILogger<ChClient> _logger = logger;

    public async Task<long> BulkInsertAsync(string tableName, System.Data.IDataReader reader, int batchSize = 10000)
    {
        var rowsWritten = await this.WithConnectionAsync(async c => {
            try
            {
                using var bulkInsert = new ClickHouseBulkCopy(c)
                {
                    DestinationTableName = tableName,
                    BatchSize = batchSize,
                    MaxDegreeOfParallelism = 1,
                };

                await bulkInsert.InitAsync();
                await bulkInsert.WriteToServerAsync(reader);

                return bulkInsert.RowsWritten;
            }
            catch (Exception exc)
            {
                _logger.LogError(exc, "Failed in BulkCopy {table}", tableName);
                throw;
            }
        });

        return rowsWritten;
    }

    public async Task ExecuteAsync(string sqlStatement)
    {
        _ = await this.WithConnectionAsync(async c => {
            if (sqlStatement.Contains("CREATE TABLE", StringComparison.CurrentCultureIgnoreCase))
            {
                _logger.LogDebug("executing: {sqlStatement}", sqlStatement);
            }

            var sw = Stopwatch.StartNew();
            var resultInt = await c.ExecuteStatementAsync(sqlStatement);
            _logger.LogDebug("Took {durationSeconds}[sec] executing: {sqlStatement}", sw.Elapsed.TotalSeconds, sqlStatement);

            return resultInt;
        });
    }

    public async Task<bool> HasTableAsync(string tableName)
    {
        try
        {
            var hasColumns = await this.WithConnectionAsync(async connection => {
                using ClickHouseDataReader reader = (ClickHouseDataReader)(await connection.ExecuteReaderAsync($"SELECT * FROM {tableName} WHERE 1=0"));
                var columnNames = (from c in reader.GetColumnNames()
                                   select c.EncloseColumnName());

                return columnNames.Any();
            });

            return hasColumns;
        }
        catch (ClickHouse.Client.ClickHouseServerException exc)
        {
            if (exc.ErrorCode == 60)
            {
                _logger.LogWarning("Table: {tableName} missing in Clickhouse", tableName);
                return false;
            }

            throw;
        }
    }

    protected async Task<T> WithConnectionAsync<T>(Func<ClickHouseConnection, Task<T>> getData)
    {
        try
        {
            using var connection = _chSession.ConnectionUnmanaged;
            connection.Logger = _logger;
            return await getData(connection);
        }
        catch (TimeoutException ex)
        {
            throw new Exception(string.Format("{0}.WithConnection() experienced a Clickhouse SQL timeout", GetType().FullName), ex);
        }
    }
}
