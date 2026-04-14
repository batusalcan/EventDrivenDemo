using System.Collections.Concurrent;

namespace EventDrivenDemo.NotificationApi.Services;

public class NotificationEventLogStore
{
    private readonly ConcurrentQueue<string> _entries = new();
    private const int MaxEntries = 200;

    public void Add(string entry)
    {
        _entries.Enqueue($"[{DateTime.UtcNow:HH:mm:ss}] {entry}");

        while (_entries.Count > MaxEntries)
            _entries.TryDequeue(out _);
    }

    public IReadOnlyList<string> GetAll() => _entries.ToArray();

    public void Clear()
    {
        while (_entries.TryDequeue(out _)) { }
    }
}
