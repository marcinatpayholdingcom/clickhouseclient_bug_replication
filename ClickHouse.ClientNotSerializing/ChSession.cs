namespace ClickHouse.ClientNotSerializing;

using ClickHouse.Client.ADO;

public interface IChSessionn
{
    ClickHouseConnection ConnectionUnmanaged { get; }
}

public class ChSession(string connectionString) : IChSessionn
{
    private readonly string _connectionString = connectionString;

    public ClickHouseConnection ConnectionUnmanaged
    {
        get
        {
            var connection = new ClickHouseConnection(_connectionString);
            return connection;
        }
    }
}
