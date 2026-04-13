using System.Collections.Concurrent;

namespace EventDrivenDemo.Api.Services;

public class EventLogStore
{
    private readonly ConcurrentQueue<string> _entries = new();
    private const int MaxEntries = 200;

    public void Add(string entry)
    {
        _entries.Enqueue($"[{DateTime.UtcNow:HH:mm:ss}] {entry}");

        // Trim oldest entries once the cap is exceeded
        while (_entries.Count > MaxEntries)
            _entries.TryDequeue(out _);
    }

    public IReadOnlyList<string> GetAll() => _entries.ToArray();

    public void Clear()
    {
        while (_entries.TryDequeue(out _)) { }
    }
}
