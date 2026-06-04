package io.openvidu.server.rest;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.openvidu.server.core.Participant;
import io.openvidu.server.core.Session;
import io.openvidu.server.core.SessionManager;
import io.openvidu.server.recording.service.EvidenceRecordingService;
import io.openvidu.server.recording.service.EvidenceRecordingService.SnapshotResult;

@RestController
@RequestMapping(RequestMappings.API + "/evidence")
public class EvidenceRestController {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private EvidenceRecordingService evidenceRecordingService;

    @RequestMapping(value = "/ping", method = RequestMethod.GET)
    public Map<String, Object> ping() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", true);
        result.put("module", "evidence");
        result.put("version", "2.19.0-evidence-test");
        return result;
    }

    @RequestMapping(value = "/start/{sessionId}/{connectionId}", method = RequestMethod.POST)
    public ResponseEntity<?> startEvidence(
            @PathVariable("sessionId") String sessionId,
            @PathVariable("connectionId") String connectionId) {

        Session session = this.sessionManager.getSessionWithNotActive(sessionId);

        if (session == null) {
            return new ResponseEntity<>("session-not-found", HttpStatus.NOT_FOUND);
        }

        Participant participant = session.getParticipantByPublicId(connectionId);

        if (participant == null) {
            return new ResponseEntity<>("participant-not-found", HttpStatus.NOT_FOUND);
        }

        String result = evidenceRecordingService.start(session, participant);

        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/stop/{sessionId}/{connectionId}", method = RequestMethod.POST)
    public ResponseEntity<?> stopEvidence(
            @PathVariable("sessionId") String sessionId,
            @PathVariable("connectionId") String connectionId) {

        String result = evidenceRecordingService.stop(sessionId, connectionId);

        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/start-player/{sessionId}/{playerNumber}", method = RequestMethod.POST)
    public ResponseEntity<?> startPlayerEvidence(
            @PathVariable("sessionId") String sessionId,
            @PathVariable("playerNumber") int playerNumber) {

        Session session = this.sessionManager.getSessionWithNotActive(sessionId);

        if (session == null) {
            return new ResponseEntity<>("session-not-found", HttpStatus.NOT_FOUND);
        }

        Participant participant = findNewestParticipantByPlayerNumber(session, playerNumber);

        if (participant == null) {
            return new ResponseEntity<>("player-not-found-or-not-active", HttpStatus.NOT_FOUND);
        }

        String result = evidenceRecordingService.startForPlayer(session, participant, playerNumber);

        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/stop-player/{sessionId}/{playerNumber}", method = RequestMethod.POST)
    public ResponseEntity<?> stopPlayerEvidence(
            @PathVariable("sessionId") String sessionId,
            @PathVariable("playerNumber") int playerNumber) {

        String result = evidenceRecordingService.stopPlayer(sessionId, playerNumber);

        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/snapshot/{roomId}/{playerNumber}/{startEpoch}/{stopEpoch}", method = { RequestMethod.GET,
            RequestMethod.POST })
    public ResponseEntity<?> snapshot(
            @PathVariable("roomId") String roomId,
            @PathVariable("playerNumber") int playerNumber,
            @PathVariable("startEpoch") long startEpoch,
            @PathVariable("stopEpoch") long stopEpoch) {

        SnapshotResult result = evidenceRecordingService.buildSnapshot(roomId, playerNumber, startEpoch, stopEpoch);

        if (result.ready) {
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION,
                            "attachment; filename=\"" + result.file.getName() + "\"")
                    .contentLength(result.file.length())
                    .contentType(MediaType.parseMediaType("audio/webm"))
                    .body((Resource) result.resource);
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", false);
        body.put("error", result.error);

        if (result.retry) {
            body.put("retryAfterSeconds", result.retryAfterSeconds);
            return ResponseEntity.status(result.httpStatus)
                    .header(HttpHeaders.RETRY_AFTER, String.valueOf(result.retryAfterSeconds))
                    .body(body);
        }

        return ResponseEntity.status(result.httpStatus).body(body);
    }

    @RequestMapping(value = "/debug/players/{sessionId}", method = RequestMethod.GET)
    public ResponseEntity<?> debugPlayers(@PathVariable("sessionId") String sessionId) {

        Session session = this.sessionManager.getSessionWithNotActive(sessionId);

        if (session == null) {
            return new ResponseEntity<>("session-not-found", HttpStatus.NOT_FOUND);
        }

        JsonArray result = new JsonArray();

        for (Participant participant : session.getParticipants()) {
            JsonObject item = new JsonObject();

            item.addProperty("connectionId", participant.getParticipantPublicId());
            item.addProperty("isStreaming", participant.isStreaming());

            try {
                JsonObject pJson = participant.toJson();
                item.add("participantJson", pJson);

                PlayerClientInfo info = parsePlayerClientInfo(participant);
                item.addProperty("metadata", info.metadata);
                item.addProperty("clientData", info.clientData);
                item.addProperty("playerNumber", info.playerNumber);
                item.addProperty("created", info.created);

            } catch (Exception e) {
                item.addProperty("parseError", e.getMessage());
            }

            result.add(item);
        }

        return new ResponseEntity<>(result.toString(), HttpStatus.OK);
    }

    private Participant findNewestParticipantByPlayerNumber(Session session, int playerNumber) {
        Participant best = null;
        long bestCreated = Long.MIN_VALUE;

        for (Participant participant : session.getParticipants()) {
            try {
                PlayerClientInfo info = parsePlayerClientInfo(participant);

                if (info.playerNumber == playerNumber && info.created > bestCreated) {
                    best = participant;
                    bestCreated = info.created;
                }
            } catch (Exception ignored) {
                // Ignore participants with unexpected/missing clientData
            }
        }

        return best;
    }

    private PlayerClientInfo parsePlayerClientInfo(Participant participant) {
        JsonObject pJson = participant.toJson();

        String metadata = pJson.get("clientData").getAsString();
        JsonObject metadataJson = JsonParser.parseString(metadata).getAsJsonObject();

        String clientData = metadataJson.get("clientData").getAsString();
        String[] parts = clientData.split("\\.");

        PlayerClientInfo info = new PlayerClientInfo();
        info.metadata = metadata;
        info.clientData = clientData;
        info.playerNumber = Integer.parseInt(parts[0]);
        info.created = Long.parseLong(parts[1]);

        return info;
    }

    private static class PlayerClientInfo {
        String metadata;
        String clientData;
        int playerNumber;
        long created;
    }
}
