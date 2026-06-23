package org.flowbridge.dashboard;

import org.flowbridge.core.application.port.in.EventBus;
import org.flowbridge.core.application.port.out.DeadLetterStore;
import org.flowbridge.core.application.port.out.ReplayableStore;
import org.flowbridge.core.domain.model.DeadLetterRecord;
import org.flowbridge.core.domain.model.Event;
import org.flowbridge.core.domain.model.ReplayOptions;
import org.flowbridge.core.application.service.DefaultEventBus;
import org.flowbridge.core.infrastructure.serialization.Serializer;
import org.flowbridge.embedded.RocksDBReplayableStore;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/flowbridge")
public class FlowBridgeDashboardController {

    private final EventBus eventBus;
    private final Serializer serializer;
    private final ReplayableStore replayableStore;
    private final DeadLetterStore deadLetterStore;

    public FlowBridgeDashboardController(EventBus eventBus, Serializer serializer,
                                         ReplayableStore replayableStore, DeadLetterStore deadLetterStore) {
        this.eventBus = eventBus;
        this.serializer = serializer;
        this.replayableStore = replayableStore;
        this.deadLetterStore = deadLetterStore;
    }

    @GetMapping
    public String showDashboard(Model model) {
        // Collect subscribed topics from DefaultEventBus
        Set<String> subscribedTopics = new HashSet<>();
        if (eventBus instanceof DefaultEventBus) {
            subscribedTopics.addAll(((DefaultEventBus) eventBus).getSubscribedTopics());
        }

        // Collect all persisted topics from RocksDB store if available
        Set<String> persistedTopics = new HashSet<>();
        long totalMessages = 0;
        if (replayableStore instanceof RocksDBReplayableStore) {
            List<String> rocksDbTopics = ((RocksDBReplayableStore) replayableStore).listTopics();
            persistedTopics.addAll(rocksDbTopics);
            for (String topic : rocksDbTopics) {
                totalMessages += replayableStore.findByTopic(topic).size();
            }
        }

        // Merge to find all unique topics
        Set<String> allTopics = new HashSet<>();
        allTopics.addAll(subscribedTopics);
        allTopics.addAll(persistedTopics);

        // Fetch DLQ contents
        List<DeadLetterRecord> dlqRecords = new ArrayList<>();
        if (deadLetterStore != null) {
            dlqRecords = deadLetterStore.findAll();
        }

        // Prepare topic details
        List<Map<String, Object>> topicDetails = allTopics.stream().map(topic -> {
            Map<String, Object> map = new HashMap<>();
            map.put("name", topic);
            map.put("isSubscribed", subscribedTopics.contains(topic));
            map.put("isPersisted", persistedTopics.contains(topic));
            
            long count = 0;
            if (replayableStore != null) {
                count = replayableStore.findByTopic(topic).size();
            }
            map.put("messageCount", count);
            return map;
        }).collect(Collectors.toList());

        model.addAttribute("topics", topicDetails);
        model.addAttribute("dlqRecords", dlqRecords);
        model.addAttribute("totalTopics", allTopics.size());
        model.addAttribute("totalMessages", totalMessages);
        model.addAttribute("dlqSize", dlqRecords.size());
        model.addAttribute("provider", replayableStore != null ? "Embedded (RocksDB)" : "Local (In-Memory)");

        return "flowbridge/dashboard";
    }

    @PostMapping("/dlq/retry")
    public String retryDlq(@RequestParam("eventId") String eventId, RedirectAttributes redirectAttributes) {
        if (deadLetterStore == null) {
            redirectAttributes.addFlashAttribute("error", "Dead Letter Store is not configured.");
            return "redirect:/flowbridge";
        }

        try {
            DeadLetterRecord targetRecord = deadLetterStore.findAll().stream()
                    .filter(r -> r.getEvent().getId().equals(eventId))
                    .findFirst()
                    .orElse(null);

            if (targetRecord == null) {
                redirectAttributes.addFlashAttribute("error", "Event not found in Dead Letter Queue.");
                return "redirect:/flowbridge";
            }

            Event event = targetRecord.getEvent();
            Class<?> clazz = Class.forName(event.getPayloadType());
            Object payload = serializer.deserialize(event.getPayload(), clazz);

            // Republish back onto the event bus
            eventBus.publish(event.getTopic(), payload, event.getHeaders());

            // Delete from DLQ store
            deadLetterStore.delete(eventId);

            redirectAttributes.addFlashAttribute("success", "Successfully retried event " + eventId + " and deleted DLQ entry.");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", "Failed to retry event: " + e.getMessage());
        }
        return "redirect:/flowbridge";
    }

    @PostMapping("/dlq/delete")
    public String deleteDlq(@RequestParam("eventId") String eventId, RedirectAttributes redirectAttributes) {
        if (deadLetterStore == null) {
            redirectAttributes.addFlashAttribute("error", "Dead Letter Store is not configured.");
            return "redirect:/flowbridge";
        }

        try {
            deadLetterStore.delete(eventId);
            redirectAttributes.addFlashAttribute("success", "Deleted DLQ entry for event " + eventId);
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", "Failed to delete DLQ entry: " + e.getMessage());
        }
        return "redirect:/flowbridge";
    }

    @PostMapping("/replay")
    public String triggerReplay(@RequestParam("topic") String topic,
                                @RequestParam("replayType") String replayType,
                                @RequestParam(value = "offset", required = false) Long offset,
                                @RequestParam(value = "timestamp", required = false) String timestampStr,
                                RedirectAttributes redirectAttributes) {
        try {
            ReplayOptions options;
            if ("ALL".equalsIgnoreCase(replayType)) {
                options = ReplayOptions.all();
            } else if ("FROM_OFFSET".equalsIgnoreCase(replayType)) {
                if (offset == null) {
                    redirectAttributes.addFlashAttribute("error", "Offset must be specified for FROM_OFFSET replay.");
                    return "redirect:/flowbridge";
                }
                options = ReplayOptions.fromOffset(offset);
            } else if ("FROM_TIMESTAMP".equalsIgnoreCase(replayType)) {
                if (timestampStr == null || timestampStr.trim().isEmpty()) {
                    redirectAttributes.addFlashAttribute("error", "Timestamp must be specified for FROM_TIMESTAMP replay.");
                    return "redirect:/flowbridge";
                }
                // Expecting ISO instant format or long milliseconds
                Instant instant;
                try {
                    instant = Instant.parse(timestampStr);
                } catch (Exception e) {
                    try {
                        instant = Instant.ofEpochMilli(Long.parseLong(timestampStr));
                    } catch (Exception parseEx) {
                        redirectAttributes.addFlashAttribute("error", "Invalid timestamp format. Use ISO format (e.g. 2026-06-23T06:00:00Z) or epoch milliseconds.");
                        return "redirect:/flowbridge";
                    }
                }
                options = ReplayOptions.fromTimestamp(instant);
            } else {
                redirectAttributes.addFlashAttribute("error", "Unknown replay type: " + replayType);
                return "redirect:/flowbridge";
            }

            eventBus.replay(topic, options);
            redirectAttributes.addFlashAttribute("success", "Triggered replay for topic '" + topic + "' successfully.");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", "Failed to run replay: " + e.getMessage());
        }
        return "redirect:/flowbridge";
    }
}
