using MassTransit;
using MassTransitDemo.Contracts;

namespace MassTransitDemo.Server;

internal class BaseConsumer<TMessage> : IConsumer<TMessage> where TMessage : BaseEvent
{
    private readonly string _name;

    public BaseConsumer(string name)
    {
        _name = name;
    }

    public Task Consume(ConsumeContext<TMessage> context)
    {
        Console.WriteLine("[{0}] {1}", context.Message.Id, _name);
        return Task.CompletedTask;
    }
}
