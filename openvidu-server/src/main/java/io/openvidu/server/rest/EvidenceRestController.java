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

    @RequestMapping(value = "/start-user/{sessionId}/{userIdentity}", method = RequestMethod.POST)
    public ResponseEntity<?> startUserEvidence(
            @PathVariable("sessionId") String sessionId,
            @PathVariable("userIdentity") String userIdentity) {

        Integer userId = parseUserIdentity(userIdentity);
        if (userId == null) {
            return new ResponseEntity<>("invalid-user-identity", HttpStatus.BAD_REQUEST);
        }

        Session session = this.sessionManager.getSessionWithNotActive(sessionId);

        if (session == null) {
            return new ResponseEntity<>("session-not-found", HttpStatus.NOT_FOUND);
        }

        Participant participant = findNewestParticipantByUserId(session, userId);

        if (participant == null) {
            return new ResponseEntity<>("user-not-found-or-not-active", HttpStatus.NOT_FOUND);
        }

        String result = evidenceRecordingService.startForUser(session, participant, userId);

        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/stop-user/{sessionId}/{userIdentity}", method = RequestMethod.POST)
    public ResponseEntity<?> stopUserEvidence(
            @PathVariable("sessionId") String sessionId,
            @PathVariable("userIdentity") String userIdentity) {

        Integer userId = parseUserIdentity(userIdentity);
        if (userId == null) {
            return new ResponseEntity<>("invalid-user-identity", HttpStatus.BAD_REQUEST);
        }

        String result = evidenceRecordingService.stopUser(sessionId, userId);

        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/snapshot/{roomId}/{userIdentity}/{startEpoch}/{stopEpoch}", method = { RequestMethod.GET,
            RequestMethod.POST })
    public ResponseEntity<?> snapshot(
            @PathVariable("roomId") String roomId,
            @PathVariable("userIdentity") String userIdentity,
            @PathVariable("startEpoch") long startEpoch,
            @PathVariable("stopEpoch") long stopEpoch) {

        SnapshotResult result = evidenceRecordingService.buildSnapshot(roomId, userIdentity, startEpoch, stopEpoch);

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

                EvidenceIdentityInfo info = parseEvidenceIdentityInfo(participant);
                item.addProperty("clientMetadata", info.clientMetadata);
                item.addProperty("serverMetadata", info.serverMetadata);
                item.addProperty("clientData", info.clientData);
                item.addProperty("userId", info.userId);
                item.addProperty("isSpectator", info.isSpectator);
                item.addProperty("created", info.created);

            } catch (Exception e) {
                item.addProperty("parseError", e.getMessage());
            }

            result.add(item);
        }

        return new ResponseEntity<>(result.toString(), HttpStatus.OK);
    }

    private Participant findNewestParticipantByUserId(Session session, int userId) {
        Participant best = null;
        long bestCreated = Long.MIN_VALUE;

        for (Participant participant : session.getParticipants()) {
            try {
                EvidenceIdentityInfo info = parseEvidenceIdentityInfo(participant);

                if (!info.isSpectator && info.userId == userId && info.created > bestCreated) {
                    best = participant;
                    bestCreated = info.created;
                }
            } catch (Exception ignored) {
                // Ignore participants with unexpected/missing metadata
            }
        }

        return best;
    }

    private EvidenceIdentityInfo parseEvidenceIdentityInfo(Participant participant) {
        String serverMetadata = participant.getServerMetadata();
        JsonObject serverMetadataJson = JsonParser.parseString(serverMetadata).getAsJsonObject();
        int userId = serverMetadataJson.get("pid").getAsInt();

        String clientMetadata = participant.getClientMetadata();
        JsonObject clientMetadataJson = JsonParser.parseString(clientMetadata).getAsJsonObject();
        if (clientMetadataJson.has("userId") && !clientMetadataJson.get("userId").isJsonNull()
                && clientMetadataJson.get("userId").getAsInt() != userId) {
            throw new IllegalArgumentException("client/server user id mismatch");
        }

        String clientData = clientMetadataJson.get("clientData").getAsString();
        String[] parts = clientData.split("\\.");

        EvidenceIdentityInfo info = new EvidenceIdentityInfo();
        info.clientMetadata = clientMetadata;
        info.serverMetadata = serverMetadata;
        info.clientData = clientData;
        info.userId = userId;
        info.isSpectator = serverMetadataJson.has("spec") && !serverMetadataJson.get("spec").isJsonNull()
                && serverMetadataJson.get("spec").getAsBoolean();
        info.created = Long.parseLong(parts[parts.length - 1]);

        return info;
    }

    private Integer parseUserIdentity(String userIdentity) {
        if (userIdentity == null || !userIdentity.startsWith("uid-")) {
            return null;
        }
        try {
            int userId = Integer.parseInt(userIdentity.substring(4));
            return userId > 0 ? userId : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static class EvidenceIdentityInfo {
        String clientMetadata;
        String serverMetadata;
        String clientData;
        int userId;
        boolean isSpectator;
        long created;
    }
}
