using Microsoft.Extensions.Logging;
using System.Data;

namespace ClickHouse.ClientNotSerializing;


public class Main(
    ChClient csClient,
    ILogger<Main> logger)
{
    private readonly ChClient _chClient = csClient;
    private readonly ILogger _logger = logger;

    public async Task RunAsync()
    {
        // broken data, some data inserted some skipped, but no exception
        try
        {
            // check if table exists
            if (!await _chClient.HasTableAsync("THE_TEST_TABLE"))
            {
                await CreateTableAsync();
            }

            using var dataReader = GetData();
            var insertedRowsCount = await _chClient.BulkInsertAsync("THE_TEST_TABLE", dataReader);

            _logger.LogInformation("Inserted {rowsCount} rows to table {tableName}", insertedRowsCount, "THE_TEST_TABLE");
        }
        catch (Exception exc)
        {
            _logger.LogError(exc, "I really failed at bulk insert with IDataReader");
        }

        // --------------------------------------------------------------------------------------------------------
        // --------------------------------------------------------------------------------------------------------
        // -----------------------------------next try    ---------------------------------------------------------
        // --------------------------------------------------------------------------------------------------------
        // --------------------------------------------------------------------------------------------------------
        // broken data, but this time there is a serialization exception which is correct, but the whole data chunk fits into the batch size
        try
        {
            // check if table exists
            if (!await _chClient.HasTableAsync("THE_TEST_TABLE_2"))
            {
                await CreateTable2Async();
            }

            using var dataReader = GetSmallChankOfData();
            var insertedRowsCount = await _chClient.BulkInsertAsync("THE_TEST_TABLE_2", dataReader);

            _logger.LogInformation("Inserted {rowsCount} rows to table {tableName}", insertedRowsCount, "THE_TEST_TABLE_2");
        }
        catch (Exception exc)
        {
            _logger.LogError(exc, "I really failed at bulk insert with IDataReader for 2nd table");
        }
    }

    private System.Data.IDataReader GetData()
    {
        var dt = new DataTable();
        dt.Columns.Add("ID", typeof(int));
        dt.Columns.Add("ExchangeRate", typeof(decimal));
        dt.Columns.Add("ModifDate", typeof(DateTimeOffset));
        var c = dt.Columns.Add("OrderTypeID", typeof(int));
        c.AllowDBNull = true;

        dt.Rows.Add(1, 1.1m, DateTimeOffset.Now, 1);

        // broken row according to table schema
        // because OrderTypeID cannot be NULL
        dt.Rows.Add(2, 1.1m, DateTimeOffset.Now.AddMicroseconds(1), DBNull.Value); 

        var total = 0;
        for (int i = 3; i < 1000000; i++)
        {
            // some dump data
            decimal rate = 0m + (i / 100m);
            dt.Rows.Add(i, rate, DateTimeOffset.Now.AddSeconds(1), i);
            total++;
        }

        _logger.LogInformation("DataTable rows: {rowsCount} to be inserted", dt.Rows.Count);
        return dt.CreateDataReader();
    }

    private System.Data.IDataReader GetSmallChankOfData()
    {
        var dt = new DataTable();
        dt.Columns.Add("ID", typeof(int));
        dt.Columns.Add("ExchangeRate", typeof(decimal));
        dt.Columns.Add("ModifDate", typeof(DateTimeOffset));
        var c = dt.Columns.Add("OrderTypeID", typeof(int));
        c.AllowDBNull = true;

        dt.Rows.Add(1, 1.1m, DateTimeOffset.Now, 1);

        // broken row according to table schema
        // because OrderTypeID cannot be NULL
        dt.Rows.Add(2, 1.1m, DateTimeOffset.Now.AddMicroseconds(1), DBNull.Value);

        var total = 0;
        for (int i = 3; i < 10; i++) // this time adding just a few rows
        {
            // some dump data
            decimal rate = 0m + (i / 100m);
            dt.Rows.Add(i, rate, DateTimeOffset.Now.AddSeconds(1), i);
            total++;
        }

        _logger.LogInformation("DataTable rows: {rowsCount} to be inserted", dt.Rows.Count);
        return dt.CreateDataReader();
    }

    private async Task CreateTableAsync()
    {
        var createTableStatement = @"
            CREATE TABLE THE_TEST_TABLE(
	            ID int NOT NULL,
	            ExchangeRate Decimal(28, 20) NOT NULL,
                ModifDate DateTime64(2) NOT NULL,
	            OrderTypeID Int16 NOT NULL,
            )
            ENGINE = ReplacingMergeTree(ModifDate)
            ORDER BY (ID)
            ;
        ";

        await _chClient.ExecuteAsync(createTableStatement);
    }

    private async Task CreateTable2Async()
    {
        var createTableStatement = @"
            CREATE TABLE THE_TEST_TABLE_2(
	            ID int NOT NULL,
	            ExchangeRate Decimal(28, 20) NOT NULL,
                ModifDate DateTime64(2) NOT NULL,
	            OrderTypeID Int16 NOT NULL,
            )
            ENGINE = ReplacingMergeTree(ModifDate)
            ORDER BY (ID)
            ;
        ";

        await _chClient.ExecuteAsync(createTableStatement);
    }
}
