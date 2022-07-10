using MassTransit;
using MassTransitDemo.Contracts;
using MassTransitDemo.Server;
using RabbitMQ.Client;

Func<string, string, Action<IRabbitMqReceiveEndpointConfigurator>> transactionEventEndpointConfiguratorFactory = (consumerIdentifier, topic) =>
{
    return (endpoint) =>
    {
        endpoint.ConfigureConsumeTopology = false;
        endpoint.Consumer(typeof(BaseConsumer<TopicEvent>), type => Activator.CreateInstance(type, consumerIdentifier));

        endpoint.Bind<TopicEvent>(exchange =>
        {
            exchange.ExchangeType = ExchangeType.Topic;
            exchange.RoutingKey = topic;
        });
    };
};

IBusControl busControl = Bus.Factory.CreateUsingRabbitMq((cfg) =>
{
    cfg.Host(new Uri("rabbitmq://localhost:5672"), (rmq) =>
    {
        rmq.Username("guest");
        rmq.Password("guest");
    });

    cfg.ReceiveEndpoint("service-1", transactionEventEndpointConfiguratorFactory("Topic All", "*.*"));
    cfg.ReceiveEndpoint("service-2", transactionEventEndpointConfiguratorFactory("Topic A", "a.*"));
    cfg.ReceiveEndpoint("service-3", transactionEventEndpointConfiguratorFactory("Topic A1", "a.1"));
    cfg.ReceiveEndpoint("service-4", transactionEventEndpointConfiguratorFactory("Topic A2", "a.2"));
    cfg.ReceiveEndpoint("service-5", transactionEventEndpointConfiguratorFactory("Topic B", "b.*"));
    cfg.ReceiveEndpoint("service-6", transactionEventEndpointConfiguratorFactory("Topic B1", "b.1"));
    cfg.ReceiveEndpoint("service-7", transactionEventEndpointConfiguratorFactory("Topic B2", "b.2"));
    cfg.ReceiveEndpoint("service-8", transactionEventEndpointConfiguratorFactory("Topic 3", "*.3"));

    // Category must be "a"
    cfg.ReceiveEndpoint("service-9", (endpoint) =>
    {
        endpoint.ConfigureConsumeTopology = false;
        endpoint.Consumer(typeof(BaseConsumer<HeaderEvent>), type => Activator.CreateInstance(type, "Header A"));

        endpoint.Bind<HeaderEvent>(exchange =>
        {
            exchange.ExchangeType = ExchangeType.Headers;
            exchange.SetBindingArgument("category", "a");
        });
    });

    // Category must be "b" AND sub-category must be "1"
    cfg.ReceiveEndpoint("service-10", (endpoint) =>
    {
        endpoint.ConfigureConsumeTopology = false;
        endpoint.Consumer(typeof(BaseConsumer<HeaderEvent>), type => Activator.CreateInstance(type, "Header B1"));

        endpoint.Bind<HeaderEvent>(exchange =>
        {
            exchange.ExchangeType = ExchangeType.Headers;
            exchange.SetBindingArgument("category", "b");
            exchange.SetBindingArgument("subcategory", "1");
            exchange.SetBindingArgument("x-match", "all");
        });
    });

    // Category must be "c" OR sub-category must be "2"
    cfg.ReceiveEndpoint("service-11", (endpoint) =>
    {
        endpoint.ConfigureConsumeTopology = false;
        endpoint.Consumer(typeof(BaseConsumer<HeaderEvent>), type => Activator.CreateInstance(type, "Header C/2"));

        endpoint.Bind<HeaderEvent>(exchange =>
        {
            exchange.ExchangeType = ExchangeType.Headers;
            exchange.SetBindingArgument("category", "c");
            exchange.SetBindingArgument("subcategory", "2");
            exchange.SetBindingArgument("x-match", "any");
        });
    });
});

await busControl.StartAsync();

Console.WriteLine("Enter 'q' to quit.");
try
{
    do
    {
        string? input = Console.ReadLine();

        if (input is not null && input.Equals("q", StringComparison.OrdinalIgnoreCase))
        {
            break;
        }
    } while (true);
}
finally
{
    await busControl.StopAsync();
}
