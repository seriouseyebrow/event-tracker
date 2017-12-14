package oiresrones.tracking;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExactTrackingServiceTest {

    @Test
    public void testRegisterEvents() {
        ExactTrackingService service = new ExactTrackingService();
        for (int i = 0; i < 1000; i++) {
            service.registerEvent();
        }
    }

    @Test
    public void testConcurrentlyRegisterEvents() {
        ExactTrackingService service = new ExactTrackingService();
        ExecutorService es = Executors.newFixedThreadPool(10);
        Runnable registerRunnable = new Runnable() {
            @Override
            public void run() {
                service.registerEvent();
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        Callable<Long> getMC = new Callable() {
            @Override
            public Long call() {
                return service.getLastMinuteCount();
            }
        };
        Callable<Long> getHC = new Callable() {
            @Override
            public Long call() {
                return service.getLastHourCount();
            }
        };
        Callable<Long> getDC = new Callable() {
            @Override
            public Long call() {
                return service.getLastDayCount();
            }
        };
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
                es.submit(registerRunnable);
            }

            try {
                System.out.println(String.format("Minute Count:%s", es.submit(getMC).get().toString()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            try {
                System.out.println(String.format("Hour count:%s", es.submit(getHC).get().toString()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            try {
                System.out.println(String.format("Day count:%s", es.submit(getDC).get().toString()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
