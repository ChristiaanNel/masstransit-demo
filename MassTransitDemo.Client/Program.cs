using MassTransit;
using MassTransitDemo.Contracts;
using RabbitMQ.Client;

int currentId = 1;
Func<TopicEvent, string> routingKeyFormatter = ev => $"{ev.Category.ToLowerInvariant()}.{ev.SubCategory.ToLowerInvariant()}";

IBusControl busControl = Bus.Factory.CreateUsingRabbitMq((cfg) =>
{
    cfg.Host(new Uri("rabbitmq://localhost:5672"), (rmq) =>
    {
        rmq.Username("guest");
        rmq.Password("guest");
    });

    cfg.Publish<TopicEvent>(ev => ev.ExchangeType = ExchangeType.Topic);
    cfg.Send<TopicEvent>(ev => ev.UseRoutingKeyFormatter(ctx => routingKeyFormatter(ctx.Message)));

    cfg.Publish<HeaderEvent>(ev => ev.ExchangeType = ExchangeType.Headers);
});

await busControl.StartAsync();

Console.WriteLine("Enter a value (<[t(opic)|h(header)]> <Category> <Sub Category> <Message>) or 'q' to quit.");
try
{
    do
    {
        string? input = Console.ReadLine();

        if (input is null)
        {
            continue;
        }

        if (input.Equals("q", StringComparison.OrdinalIgnoreCase))
        {
            break;
        }

        string[] parts = input.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length != 3)
        {
            Console.WriteLine("Input was not in the correct format.");
            continue;
        }

        var id = currentId++;
        var type = parts[0];
        var category = parts[1];
        var subCategory = parts[2];
        
        if (type.Equals("h", StringComparison.OrdinalIgnoreCase))
        {
            await busControl.Publish<HeaderEvent>(new(
                Id: id,
                Category: category,
                SubCategory: subCategory), (ctx) =>
                {
                    ctx.Headers.Set("category", category.ToLowerInvariant());
                    ctx.Headers.Set("subcategory", subCategory.ToLowerInvariant());
                });
        }
        else if (type.Equals("t", StringComparison.OrdinalIgnoreCase))
        {
            await busControl.Publish<TopicEvent>(new(
                Id: id,
                Category: category,
                SubCategory: subCategory));
        }
        else
        {
            Console.WriteLine("Type must be either 't' or 'h'");
        }
    } while (true);
}
finally
{
    await busControl.StopAsync();
}
