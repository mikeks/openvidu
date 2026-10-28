import io.openvidu.server.rest.BotAudioTapController;
import java.lang.reflect.*;
import java.util.Map;
import java.util.List;
import java.util.concurrent.*;
import org.springframework.http.ResponseEntity;

public class TapLeaseTest {
    static long revision=0;
    static org.springframework.http.ResponseEntity<?> select(BotAudioTapController c, List<String> ids) {
        BotAudioTapController.Selection selection=new BotAudioTapController.Selection();
        selection.revision=++revision; selection.assignments=new java.util.ArrayList<>(); for(String id:ids) selection.assignments.add(BotAudioSelectionTest.assignment(id,"human",42)); return c.selectRooms(selection);
    }
    static void set(Object o,String name,Object value) throws Exception {
        Field f=(o instanceof BotAudioTapController ? BotAudioTapController.class : o.getClass()).getDeclaredField(name);f.setAccessible(true);f.set(o,value);
    }
    static Object get(Object o,String name) throws Exception {
        Field f=(o instanceof BotAudioTapController ? BotAudioTapController.class : o.getClass()).getDeclaredField(name);f.setAccessible(true);return f.get(o);
    }
    static void check(boolean ok,String label){if(!ok)throw new AssertionError(label);}
    static BotAudioTapController.Request request(int port){
        BotAudioTapController.Request r=new BotAudioTapController.Request();
        r.sessionId="room";r.connectionId="human";r.streamId="stream";r.port=port;return r;
    }
    public static void main(String[] args) throws Exception {
                io.openvidu.server.core.Session[] current = {new io.openvidu.server.core.Session("room", null, null, null)};
        BotAudioTapController c=new BotAudioTapController() {
            protected io.openvidu.server.core.Session findSession(String id) { return "room".equals(id) ? current[0] : null; }
        };
        CountDownLatch unblock=new CountDownLatch(1),entered=new CountDownLatch(1);
        ExecutorService worker=(ExecutorService)get(c,"work");
        worker.execute(()->{entered.countDown();try{unblock.await();}catch(InterruptedException e){Thread.currentThread().interrupt();}});
        entered.await();
        try {
            String a="a".repeat(32),b="b".repeat(32);
            check(c.renew(a,request(58000)).getStatusCodeValue()==503,"disabled by default");
            set(c,"enabled",true);set(c,"maxTaps",1);
            check(c.renew(a,request(58000)).getStatusCodeValue()==403,"unselected room denied");
            select(c,java.util.Arrays.asList("room"));
            check(c.renew(a,request(80)).getStatusCodeValue()==400,"restricted ports");
            check(c.renew("bad",request(58000)).getStatusCodeValue()==400,"lease validation");
            check(c.renew(a,request(58000)).getStatusCodeValue()==202,"queued without blocking");
            check(c.renew(a,request(58000)).getStatusCodeValue()==200,"idempotent renewal");
            check(c.renew(a,request(58002)).getStatusCodeValue()==409,"immutable lease identity");
            check(c.renew(b,request(58002)).getStatusCodeValue()==429,"capacity");
            Map<?,?> taps=(Map<?,?>)get(c,"taps");
            Object tap=taps.get(a);set(tap,"expires",0L);
            Method expire=BotAudioTapController.class.getDeclaredMethod("expire");expire.setAccessible(true);expire.invoke(c);
            check(taps.isEmpty(),"expired lease removed");
            check(c.renew(b,request(58002)).getStatusCodeValue()==202,"capacity recovered");
            c.remove(b);c.remove(b);
            check(taps.isEmpty(),"idempotent delete");
            check(c.renew(a,request(58000)).getStatusCodeValue()==202,"tap queued before game end");
            select(c,java.util.Collections.emptyList());
            check(taps.isEmpty(),"game end removes pending taps immediately");
            check(c.renew(a,request(58000)).getStatusCodeValue()==403,"last bot removed");
            select(c,java.util.Arrays.asList("room"));

            select(c,java.util.Arrays.asList("room"));
            Thread.sleep(2);
            current[0] = new io.openvidu.server.core.Session("room", null, null, null);
            check(c.renew(a,request(58000)).getStatusCodeValue()==202,"active game selection survives media reconnection");
            c.remove(a);
            select(c,java.util.Arrays.asList("room"));
            unblock.countDown();
            // Missing KMS/session manager must fail only this tap, not the service.
            check(c.renew(a,request(58000)).getStatusCodeValue()==202,"failure queued");
            for(int i=0;i<100;i++){
                ResponseEntity<?> status=c.renew(a,request(58000));
                if("failed".equals(((Map<?,?>)status.getBody()).get("state")))break;
                Thread.sleep(10);
            }
            check("failed".equals(((Map<?,?>)c.renew(a,request(58000)).getBody()).get("state")),"setup failure contained");
            System.out.println("PASS: default-off, validation, bounded admission, nonblocking control, renew, expiry, delete and setup failure isolation.");
        } finally {unblock.countDown();c.stop();}
    }
}