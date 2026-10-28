import io.openvidu.server.rest.BotAudioTapController;
import io.openvidu.server.core.*;
import java.util.*;
import java.lang.reflect.*;
public class BotAudioSelectionTest {
    static long revision=0;
    static BotAudioTapController.Assignment assignment(String room,String connection,int user) {
        BotAudioTapController.Assignment a=new BotAudioTapController.Assignment();
        a.sessionId=room;a.connectionId=connection;a.userId=user;a.analysisId="state-1";
        a.mode="explicit-end";a.sttMode="dry-run";a.language="en-US";a.pauseSeconds=3;a.expiresAt=Long.MAX_VALUE;
        return a;
    }
    static org.springframework.http.ResponseEntity<?> select(BotAudioTapController c,List<BotAudioTapController.Assignment> assignments) {
        BotAudioTapController.Selection s=new BotAudioTapController.Selection();s.revision=++revision;s.assignments=assignments;return c.selectRooms(s);
    }
    static class Room extends Session {
        final Map<String,Participant> people=new HashMap<>();
        Room(String id) {super(id,null,null,null);}
        public Participant getParticipantByPublicId(String id) {return people.get(id);}
    }
    static class Publisher extends Participant {
        boolean publishing=true;
        Publisher(String id) {super(null,null,id,null,null,null,null,null,null,null,null);}
        public boolean isStreaming() {return publishing;}
        public String getPublisherStreamId() {return "stream-"+getParticipantPublicId();}
    }
    static void check(boolean ok,String message) {if(!ok)throw new AssertionError(message);}
    static List<?> publishers(BotAudioTapController c) {return (List<?>)((Map<?,?>)c.publishers().getBody()).get("publishers");}
    public static void main(String[] args)throws Exception {
        Map<String,Room> rooms=new HashMap<>();Room r=new Room("room");Publisher human=new Publisher("human");
        r.people.put("human",human);r.people.put("not-selected",new Publisher("not-selected"));rooms.put("room",r);
        BotAudioTapController c=new BotAudioTapController(){protected Session findSession(String id){return rooms.get(id);}};
        try {
            var a=assignment("room","human",42);
            check(select(c,List.of(a)).getStatusCodeValue()==503,"disabled by default");
            Field enabled=BotAudioTapController.class.getDeclaredField("enabled");enabled.setAccessible(true);enabled.set(c,true);
            check(publishers(c).isEmpty(),"default deny");
            select(c,List.of(a));check(publishers(c).size()==1,"only assigned human, not entire room");
            human.publishing=false;check(publishers(c).isEmpty(),"unpublished is unavailable");human.publishing=true;
            var delayed=new BotAudioTapController.Selection();delayed.revision=revision-1;delayed.assignments=List.of();
            check(c.selectRooms(delayed).getStatusCodeValue()==409,"stale snapshot rejected");
            delayed.revision=revision;check(c.selectRooms(delayed).getStatusCodeValue()==409,"conflicting revision rejected");
            delayed.assignments=List.of(a);check(c.selectRooms(delayed).getStatusCodeValue()==200,"idempotent acknowledgement");
            var invalid=assignment("room","human",42);invalid.sttMode="other";
            check(select(c,List.of(invalid)).getStatusCodeValue()==400,"unknown recognition mode rejected");
            check(select(c,List.of(a,a)).getStatusCodeValue()==400,"duplicate assignment rejected");
            var expired=assignment("room","human",42);expired.expiresAt=1;select(c,List.of(expired));check(publishers(c).isEmpty(),"deadline fails closed");
            select(c,List.of());check(publishers(c).isEmpty(),"explicit finish removes publisher");
            rooms.remove("room");select(c,List.of(a));check(publishers(c).isEmpty(),"missing session safe");rooms.put("room",r);
            check(publishers(c).size()==1,"assignment survives late session creation");
            System.out.println("PASS: user selection, modes, deadlines, revision ordering, idempotency and missing publishers.");
        }finally{c.stop();}
    }
}
