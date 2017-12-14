package oiresrones.tracking;

public interface TrackingService {
    void registerEvent();

    long getLastMinuteCount();

    long getLastHourCount();

    long getLastDayCount();
}
