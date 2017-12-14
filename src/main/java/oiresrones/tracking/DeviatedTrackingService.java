package oiresrones.tracking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Данный вариант допускает погрешность при подсчете количеств событий. Погрешность появляется из-за того,
 * что фактический временной интервал составляет, например, для getLastMinuteCount()
 * [1минута - minuteDeviation,1минута]. Т.е. для заданного minuteDeviation=500 можно получить количество
 * событий за последние 55.5-60 секунд. Используемый метод подсчета подходит для случая с большим
 * количеством событий, когда нет возможности хранить в памяти временные метки всех событий за день.
 */
public class DeviatedTrackingService implements TrackingService {
    public static final Duration DURATION_MINUTE = Duration.of(1, ChronoUnit.MINUTES);
    public static final Duration DURATION_HOUR = Duration.of(1, ChronoUnit.HOURS);
    public static final Duration DURATION_DAY = Duration.of(1, ChronoUnit.DAYS);
    private static final Logger log = LoggerFactory.getLogger(ExactTrackingService.class);
    /**
     * счетчики событий за последнюю минуту, час, день соответственно
     */
    private final AtomicLong lastMinuteCount = new AtomicLong(0L);
    private final AtomicLong lastHourCount = new AtomicLong(0L);
    private final AtomicLong lastDayCount = new AtomicLong(0L);
    /**
     * очереди счетчиков событий за часть интервала. весь временной интервал делится на равные
     * части продолжительностью, например, для минуты, minuteDeviation (если точнее, на максимально
     * возможные одинаковые интервалы, не превышающие minuteDeviation). Через каждый такой промежуток
     * времени из начала очереди извлекается один элемент и в конец добавляется другой, обнуленный.
     * Счетчик событий за минуту равен сумме счетчиков всей очереди.
     */
    private Deque<AtomicLong> partMinuteCounters = new ConcurrentLinkedDeque<>();
    private Deque<AtomicLong> partHourCounters = new ConcurrentLinkedDeque<>();
    private Deque<AtomicLong> partDayCounters = new ConcurrentLinkedDeque<>();
    private long minuteDeviation, hourDeviation, dayDeviation;
    /**
     * моменты времени следующего "перемещения" счетчиков
     */
    private volatile LocalDateTime nextMinuteShiftTime, nextHourShiftTime, nextDayShiftTime;
    /**
     * активные счетчики, значения которых изменяются при вызове registerEvent()
     */
    private volatile AtomicLong currentMuniteCounter, currentHourCounter, currentDayCounter;


    /**
     * погрешности в мс в выборе временного диапазона для получения количества событий
     *
     * @param minuteDeviation
     * @param hourDeviation
     * @param dayDeviation
     */
    public DeviatedTrackingService(long minuteDeviation, long hourDeviation, long dayDeviation) {
        this.minuteDeviation = minuteDeviation;
        this.hourDeviation = hourDeviation;
        this.dayDeviation = dayDeviation;

        initializeforMinute();
        initializeForHour();
        initializeForDay();
    }

    private void initializeForDay() {
        LocalDateTime now = LocalDateTime.now();
        partDayCounters = new ConcurrentLinkedDeque<>();
        for (long i = 0; i < Math.ceil(DURATION_DAY.toMillis() / dayDeviation); i++) {
            partDayCounters.add(new AtomicLong(0));
            currentDayCounter = partDayCounters.getLast();
            nextDayShiftTime = now.plus(dayDeviation, ChronoUnit.MILLIS);
        }
    }

    private void initializeForHour() {
        LocalDateTime now = LocalDateTime.now();
        partHourCounters = new ConcurrentLinkedDeque<>();
        for (long i = 0; i < Math.ceil(DURATION_HOUR.toMillis() / hourDeviation); i++)
            partHourCounters.add(new AtomicLong(0));
        currentHourCounter = partHourCounters.getLast();
        nextHourShiftTime = now.plus(hourDeviation, ChronoUnit.MILLIS);
    }

    private void initializeforMinute() {
        partMinuteCounters = new ConcurrentLinkedDeque<>();
        for (long i = 0; i < Math.ceil(DURATION_MINUTE.toMillis() / minuteDeviation); i++)
            partMinuteCounters.add(new AtomicLong(0));
        currentMuniteCounter = partMinuteCounters.getLast();
        nextMinuteShiftTime = LocalDateTime.now().plus(minuteDeviation, ChronoUnit.MILLIS);

    }

    @Override
    public void registerEvent() {
        refreshCounters();
        /*
        при регистрации увеличиваются значения общего счетчика и счетчика за интервал 0-deviation.
         */
        currentMuniteCounter.incrementAndGet();
        currentHourCounter.incrementAndGet();
        currentDayCounter.incrementAndGet();
        lastMinuteCount.incrementAndGet();
        lastHourCount.incrementAndGet();
        lastDayCount.incrementAndGet();
        if (log.isInfoEnabled())
            log.info("Added event at timestamp {},thread:{}", LocalDateTime.now().format(DateTimeFormatter
                    .ISO_LOCAL_DATE_TIME), Thread
                    .currentThread().getId());
    }

    @Override
    public long getLastMinuteCount() {
        refreshMinuteCounters();
        return lastMinuteCount.get();
    }

    @Override
    public long getLastHourCount() {
        refreshHourCounters();
        return lastHourCount.get();
    }

    @Override
    public long getLastDayCount() {
        refreshDayCounters();
        return lastDayCount.get();
    }

    private void refreshCounters() {
        log.info("Start refreshing counters");
        refreshMinuteCounters();
        refreshHourCounters();
        refreshDayCounters();
        log.info("Counters are refreshed");
    }

    /**
     * обновляет счетчики для подсчета количества событий за день.
     */
    private void refreshDayCounters() {
        LocalDateTime now = LocalDateTime.now();
        /*
        если с момента регистрации последнего события прошло больше дня,
        очередь счетчиков полностью очищается
         */
        synchronized (nextDayShiftTime) {
            if (nextDayShiftTime.plus(DURATION_DAY).isBefore(now)) {
                initializeForDay();
            }
        /*
        если оказывается, что состояние очереди устарело, т.е. последний сдвиг
        счетчиков в очереди произошел ранее, чем определено интервалом dayDeviation,
        то из очереди удаляется необходимое количество элементов-счетчиков (с сопутствующим уменьшением
        значения lastDayCount), добавляются новые обнуленные элементы.
         */
            while (nextDayShiftTime.isBefore(now)) {
                currentDayCounter = new AtomicLong(0);
                partDayCounters.add(currentDayCounter);
                lastDayCount.addAndGet(partDayCounters.poll().get() * -1);
                nextDayShiftTime = nextDayShiftTime.plus(dayDeviation, ChronoUnit.MILLIS);
                if (log.isInfoEnabled())
                    log.info("Day counters are shifted. last day count:{}, next shift time:{}", lastDayCount,
                            nextDayShiftTime);
            }
        }
    }

    private void refreshHourCounters() {
        LocalDateTime now = LocalDateTime.now();
        synchronized (nextHourShiftTime) {
            if (nextHourShiftTime.plus(DURATION_HOUR).isBefore(now)) {
                initializeForHour();
            }
            while (nextHourShiftTime.isBefore(now)) {
                currentHourCounter = new AtomicLong(0);
                partHourCounters.add(currentHourCounter);
                lastHourCount.addAndGet(partHourCounters.poll().get() * -1);
                nextHourShiftTime = nextHourShiftTime.plus(hourDeviation, ChronoUnit.MILLIS);
                if (log.isInfoEnabled())
                    log.info("Hour counters are shifted. last hour count:{}, next shift time:{}", lastHourCount,
                            nextHourShiftTime);
            }
        }
    }

    private void refreshMinuteCounters() {
        LocalDateTime now = LocalDateTime.now();
        synchronized (nextMinuteShiftTime) {
            if (nextMinuteShiftTime.plus(DURATION_MINUTE).isBefore(now)) {
                initializeforMinute();
            }
            while (nextMinuteShiftTime.isBefore(now)) {
                currentMuniteCounter = new AtomicLong(0);
                partMinuteCounters.add(currentMuniteCounter);
                log.info("count before:" + lastMinuteCount.get());
                lastMinuteCount.addAndGet(partMinuteCounters.poll().get() * -1);
                log.info("count after:" + lastMinuteCount.get());
                nextMinuteShiftTime = nextMinuteShiftTime.plus(minuteDeviation, ChronoUnit.MILLIS);
                if (log.isInfoEnabled())
                    log.info("Minute counters are shifted. last minute count:{}, next shift time:{}", lastMinuteCount,
                            nextMinuteShiftTime);
            }
        }
    }
}
