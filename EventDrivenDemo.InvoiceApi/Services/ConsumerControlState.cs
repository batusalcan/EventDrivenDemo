namespace EventDrivenDemo.InvoiceApi.Services;

/// <summary>
/// Controls whether the Invoice consumer is actively processing messages.
/// Pausing disconnects from the broker so messages queue up — used to demo fault tolerance.
/// </summary>
public class ConsumerControlState
{
    private volatile bool _isPaused = false;

    public bool IsPaused => _isPaused;

    public void Pause()  => _isPaused = true;
    public void Resume() => _isPaused = false;
}
