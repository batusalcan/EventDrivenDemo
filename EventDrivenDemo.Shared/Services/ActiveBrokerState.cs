using EventDrivenDemo.Shared.Enums;

namespace EventDrivenDemo.Shared.Services;

/// <summary>
/// In-memory singleton that tracks the currently active message broker.
/// Can be updated at runtime via the SystemController without a restart.
/// </summary>
public class ActiveBrokerState
{
    // volatile ensures changes are immediately visible across all threads.
    private volatile BrokerType _current;

    public ActiveBrokerState(BrokerType initial)
    {
        _current = initial;
    }

    public BrokerType Current => _current;

    public void SwitchTo(BrokerType brokerType) => _current = brokerType;
}
