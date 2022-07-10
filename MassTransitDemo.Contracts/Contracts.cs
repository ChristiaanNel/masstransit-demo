namespace MassTransitDemo.Contracts;

public record BaseEvent(int Id);
public record TopicEvent(int Id, string Category, string SubCategory) : BaseEvent(Id);
public record HeaderEvent(int Id, string Category, string SubCategory) : BaseEvent(Id);
