using Microsoft.Extensions.DependencyInjection;

namespace ClickHouse.ClientNotSerializing;

internal class Program
{
    /// 
    /// 
    /// 
    /// 
    ///  put your CH connection data here
    const string CH_CONNECTION_STRING = @"Host=xxx.xxx.xxx.xxx;Port=8123;Username=x;Password=x;Database=x";
    /// 
    /// 
    /// 




    static async Task Main(string[] _)
    {
        Console.WriteLine("Hello, World!");

        var services = ConfigureServices();

        var app = services.GetRequiredService<Main>();
        await app.RunAsync();
    }

    private static ServiceProvider ConfigureServices()
    {
        var services = new ServiceCollection();

        services.AddLogging();
        services.AddSingleton<IChSessionn, ChSession>(x =>
        {
            return new ChSession(CH_CONNECTION_STRING);
        });
        services.AddSingleton<ChClient>();
        services.AddSingleton<Main>();

        return services.BuildServiceProvider();
    }
}
