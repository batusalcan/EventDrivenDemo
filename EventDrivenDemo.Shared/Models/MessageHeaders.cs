namespace EventDrivenDemo.Shared.Models;

public class MessageHeaders : Dictionary<string, string>
{
    public static MessageHeaders Empty => new();

    public static MessageHeaders For(string key, string value) => new() { { key, value } };
}
