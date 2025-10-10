package state

import (
	"log"
	"sync"
	"time"

	"github.com/sst/opencode/internal/interfaces"
	"github.com/sst/opencode/internal/types"
)

// EventBus manages event distribution across panels
type EventBus struct {
	subscribers    map[string]chan types.StateEvent
	subscriberMeta map[string]interfaces.SubscriberInfo
	mutex          sync.RWMutex
	eventHistory   []types.StateEvent
	maxHistory     int
}

// NewEventBus creates a new event bus for state notifications
func NewEventBus(maxHistory int) *EventBus {
	return &EventBus{
		subscribers:    make(map[string]chan types.StateEvent),
		subscriberMeta: make(map[string]interfaces.SubscriberInfo),
		eventHistory:   make([]types.StateEvent, 0, maxHistory),
		maxHistory:     maxHistory,
	}
}

// Subscribe registers a panel for state change notifications
func (bus *EventBus) Subscribe(panelID, panelType string, eventChan chan types.StateEvent) {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()

	bus.subscribers[panelID] = eventChan
	bus.subscriberMeta[panelID] = interfaces.SubscriberInfo{
		PanelID:     panelID,
		PanelType:   panelType,
		ConnectedAt: time.Now(),
		EventCount:  0,
	}

	log.Printf("Panel %s (%s) subscribed to events", panelID, panelType)

	// Notify other panels about new connection
	connectEvent := types.StateEvent{
		ID:          generateEventID(),
		Type:        types.EventPanelConnected,
		Data:        types.PanelConnectionPayload{PanelID: panelID, PanelType: panelType},
		SourcePanel: "system",
		Timestamp:   time.Now(),
	}
	bus.broadcastUnsafe(connectEvent, panelID)
}

// Unsubscribe removes a panel from event notifications
func (bus *EventBus) Unsubscribe(panelID string) {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()

	if eventChan, exists := bus.subscribers[panelID]; exists {
		// Close the channel to signal shutdown
		close(eventChan)
		delete(bus.subscribers, panelID)

		// Get panel info before deleting
		panelInfo := bus.subscriberMeta[panelID]
		delete(bus.subscriberMeta, panelID)

		log.Printf("Panel %s (%s) unsubscribed from events", panelID, panelInfo.PanelType)

		// Notify other panels about disconnection
		disconnectEvent := types.StateEvent{
			ID:          generateEventID(),
			Type:        types.EventPanelDisconnected,
			Data:        types.PanelConnectionPayload{PanelID: panelID, PanelType: panelInfo.PanelType},
			SourcePanel: "system",
			Timestamp:   time.Now(),
		}
		bus.broadcastUnsafe(disconnectEvent, panelID)
	}
}

// Broadcast sends events to all registered panels except the source
func (bus *EventBus) Broadcast(event types.StateEvent) {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()

	bus.broadcastUnsafe(event, event.SourcePanel)
}

// broadcastUnsafe sends events without acquiring locks (caller must hold lock)
func (bus *EventBus) broadcastUnsafe(event types.StateEvent, excludePanel string) {
	// Add to event history
	bus.addToHistoryUnsafe(event)

	// Send to all subscribers except the source panel
	for panelID, eventChan := range bus.subscribers {
		if panelID != excludePanel {
			// Update subscriber metadata
			if meta, exists := bus.subscriberMeta[panelID]; exists {
				meta.LastEventAt = time.Now()
				meta.EventCount++
				bus.subscriberMeta[panelID] = meta
			}

			// Try to send event (non-blocking)
			select {
			case eventChan <- event:
				// Event delivered successfully
			default:
				// Channel full, log warning but continue
				log.Printf("Warning: Event channel full for panel %s, dropping event %s",
					panelID, event.Type)
			}
		}
	}
}

// BroadcastToPanel sends an event specifically to one panel
func (bus *EventBus) BroadcastToPanel(event types.StateEvent, targetPanel string) {
	bus.mutex.RLock()
	defer bus.mutex.RUnlock()

	if eventChan, exists := bus.subscribers[targetPanel]; exists {
		// Update subscriber metadata
		if meta, exists := bus.subscriberMeta[targetPanel]; exists {
			meta.LastEventAt = time.Now()
			meta.EventCount++
			bus.subscriberMeta[targetPanel] = meta
		}

		select {
		case eventChan <- event:
			// Event delivered successfully
		default:
			log.Printf("Warning: Event channel full for panel %s, dropping targeted event %s",
				targetPanel, event.Type)
		}
	}
}

// GetSubscribers returns information about all current subscribers
func (bus *EventBus) GetSubscribers() map[string]interfaces.SubscriberInfo {
	bus.mutex.RLock()
	defer bus.mutex.RUnlock()

	subscribers := make(map[string]interfaces.SubscriberInfo)
	for panelID, info := range bus.subscriberMeta {
		subscribers[panelID] = info
	}
	return subscribers
}

// GetEventHistory returns recent events from the history buffer
func (bus *EventBus) GetEventHistory(maxEvents int) []types.StateEvent {
	bus.mutex.RLock()
	defer bus.mutex.RUnlock()

	historyLen := len(bus.eventHistory)
	if maxEvents <= 0 || maxEvents > historyLen {
		maxEvents = historyLen
	}

	// Return the most recent events
	startIndex := historyLen - maxEvents
	events := make([]types.StateEvent, maxEvents)
	copy(events, bus.eventHistory[startIndex:])
	return events
}

// addToHistoryUnsafe adds an event to the history buffer (caller must hold lock)
func (bus *EventBus) addToHistoryUnsafe(event types.StateEvent) {
	bus.eventHistory = append(bus.eventHistory, event)

	// Maintain maximum history size
	if len(bus.eventHistory) > bus.maxHistory {
		// Remove oldest events
		copy(bus.eventHistory, bus.eventHistory[1:])
		bus.eventHistory = bus.eventHistory[:bus.maxHistory]
	}
}

// CreateEventFromUpdate converts a state update to a state event
func CreateEventFromUpdate(update types.StateUpdate, version int64) types.StateEvent {
	var eventType types.StateEventType

	// Map update types to event types
	switch update.Type {
	case types.SessionChanged:
		eventType = types.EventSessionChanged
	case types.SessionAdded:
		eventType = types.EventSessionAdded
	case types.SessionDeleted:
		eventType = types.EventSessionDeleted
	case types.SessionUpdated:
		eventType = types.EventSessionUpdated
	case types.MessageAdded:
		eventType = types.EventMessageAdded
	case types.MessageUpdated:
		eventType = types.EventMessageUpdated
	case types.MessageDeleted:
		eventType = types.EventMessageDeleted
	case types.InputUpdated:
		eventType = types.EventInputUpdated
	case types.CursorMoved:
		eventType = types.EventCursorMoved
	case types.ThemeChanged:
		eventType = types.EventThemeChanged
	case types.ModelChanged:
		eventType = types.EventModelChanged
	case types.AgentChanged:
		eventType = types.EventAgentChanged
	default:
		eventType = types.EventStateSync
	}

	return types.StateEvent{
		ID:          generateEventID(),
		Type:        eventType,
		Data:        update.Payload,
		Version:     version,
		SourcePanel: update.SourcePanel,
		Timestamp:   time.Now(),
	}
}

// generateEventID creates a unique identifier for events
func generateEventID() string {
	// Simple timestamp-based ID for now
	// In production, consider using UUID or other unique ID generation
	return time.Now().Format("20060102150405.000000")
}