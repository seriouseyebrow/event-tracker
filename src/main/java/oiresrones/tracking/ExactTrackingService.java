package oiresrones.tracking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * В данной реализации используется двусвязный список, в котором хранятся временные метки всех
 * событий за последний день. Более давние метки периодически удаляются в потоке thread.
 * Вводятся три указателя на самые старые метки из соответствующих интервалов. Указатели актуализируются
 * в методе refreshCounters().
 */
public class ExactTrackingService implements TrackingService {
    /*
    интервал между удалением старых меток из списка
     */
    public static final int GARBAGE_COLLECTION_INTERVAL = 1000;
    public static final Duration DURATION_MINUTE = Duration.of(1, ChronoUnit.MINUTES);
    public static final Duration DURATION_HOUR = Duration.of(1, ChronoUnit.HOURS);
    public static final Duration DURATION_DAY = Duration.of(1, ChronoUnit.DAYS);
    private static final Logger log = LoggerFactory.getLogger(ExactTrackingService.class);
    /*
    счетчики событий
     */
    private final AtomicLong lastMinuteCount = new AtomicLong(0L);
    private final AtomicLong lastHourCount = new AtomicLong(0L);
    private final AtomicLong lastDayCount = new AtomicLong(0L);
    /*
    указатели на самые старые временные метки для соответствующего интервала
     */
    private volatile LocalDateTime minutePointer;
    private volatile LocalDateTime hourPointer;
    private volatile LocalDateTime dayPointer;
    private volatile Iterator<LocalDateTime> minuteIterator;
    private volatile Iterator<LocalDateTime> hourIterator;
    private volatile Iterator<LocalDateTime> dayIterator;
    private Queue<LocalDateTime> events = new ConcurrentLinkedQueue<>();
    private Thread thread = createGarbageThread();

    public ExactTrackingService() {
        thread.start();
    }

    @Override
    public void registerEvent() {
        LocalDateTime now = LocalDateTime.now();
        //можно включить синхронизацию, а также сделать синхронизированным метод refreshCounters(),
        //если нужно добиться полного соответствия между значениями счетчиков и содержимым списка,
        //но, на мой взгляд, в этом нет необходимости
        //        synchronized (this) {
        events.add(now);
        if (log.isInfoEnabled())
            log.info("Added event at timestamp {}", now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        lastMinuteCount.incrementAndGet();
        lastHourCount.incrementAndGet();
        lastDayCount.incrementAndGet();
//        }
        if (thread.isInterrupted()) {
            thread = createGarbageThread();
            thread.start();
        }
    }

    @Override
    public long getLastMinuteCount() {
        refreshCounters();
        return lastMinuteCount.get();
    }

    @Override
    public long getLastHourCount() {
        refreshCounters();
        return lastHourCount.get();
    }

    @Override
    public long getLastDayCount() {
        refreshCounters();
        return lastDayCount.get();
    }

    //    private synchronized void refreshCounters() {
    private void refreshCounters() {
        log.info("Start refreshing counters");
        if (minutePointer == null) {
            minutePointer = events.peek();
            if (minutePointer != null)
                minuteIterator = events.iterator();
        }
        if (hourPointer == null) {
            hourPointer = events.peek();
            if (hourPointer != null)
                hourIterator = events.iterator();
        }
        if (dayPointer == null) {
            dayPointer = events.peek();
            if (dayPointer != null)
                dayIterator = events.iterator();
        }
        LocalDateTime now = LocalDateTime.now();
        /*
        сдвигает указатель на самую старую метку в списке, но отстоящую от текущего времени не более,
        чем на минуту
         */
        while (minutePointer != null && minutePointer.plus(DURATION_MINUTE).isBefore(now)) {
            minutePointer = minuteIterator.next();
            lastMinuteCount.decrementAndGet();
            if (log.isInfoEnabled())
                log.info(String.format("Minute count=%s, pointer=%s", lastMinuteCount.get(), minutePointer.format
                        (DateTimeFormatter.ISO_LOCAL_DATE_TIME)));
        }
        while (hourPointer != null && hourPointer.plus(DURATION_HOUR).isBefore(now)) {
            hourPointer = hourIterator.next();
            lastHourCount.decrementAndGet();
            if (log.isInfoEnabled())
                log.info(String.format("Hour count=%s, pointer=%s", lastHourCount.get(), hourPointer.format
                        (DateTimeFormatter
                                .ISO_LOCAL_DATE_TIME)));
        }
        while (dayPointer != null && dayPointer.plus(DURATION_DAY).isBefore(now)) {
            dayPointer = dayIterator.next();
            lastDayCount.decrementAndGet();
            if (log.isInfoEnabled())
                log.info(String.format("Day count=%s, pointer=%s", lastDayCount.get(), dayPointer.format
                        (DateTimeFormatter
                                .ISO_LOCAL_DATE_TIME)));
        }
        log.info("Counters are refreshed");

    }

    /**поток очищает список от элементов, внесенных более дня назад.
     * @return
     */
    private Thread createGarbageThread() {
        return new Thread(() ->
        {
            while (true) {
                refreshCounters();
                LocalDateTime last = events.peek();
                long collectedCount = 0;
                while (dayPointer != null && dayPointer != last) {
                    last = events.poll();
                    collectedCount++;
                }
                if (log.isInfoEnabled())
                    log.info("Collected {} events, last event timestamp is:{}", collectedCount, last
                            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                try {
                    Thread.sleep(GARBAGE_COLLECTION_INTERVAL);
                } catch (InterruptedException e) {
                    log.error("Error", e);
                }
            }
        }
        );
    }
}
