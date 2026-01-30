package ocel

import (
	"context"
	"testing"
	"time"
)

// --- In-memory Log model tests ---

func TestNewLog(t *testing.T) {
	log := NewLog()
	if log == nil {
		t.Fatal("NewLog returned nil")
	}
	if len(log.Events) != 0 {
		t.Errorf("new log events = %d, want 0", len(log.Events))
	}
	if len(log.Objects) != 0 {
		t.Errorf("new log objects = %d, want 0", len(log.Objects))
	}
}

func TestLogAddEvent(t *testing.T) {
	log := NewLog()
	ev := &Event{
		ID:        "e1",
		Type:      "Create Order",
		Timestamp: time.Now(),
		Attributes: map[string]interface{}{
			"amount": 100.0,
		},
	}
	log.AddEvent(ev)

	if len(log.Events) != 1 {
		t.Fatalf("events count = %d, want 1", len(log.Events))
	}
	if log.Events["e1"] != ev {
		t.Error("event not stored correctly")
	}
}

func TestLogAddObject(t *testing.T) {
	log := NewLog()
	obj := &Object{
		ID:   "o1",
		Type: "order",
		Attributes: map[string]interface{}{
			"status": "new",
		},
	}
	log.AddObject(obj)

	if len(log.Objects) != 1 {
		t.Fatalf("objects count = %d, want 1", len(log.Objects))
	}
	if log.Objects["o1"] != obj {
		t.Error("object not stored correctly")
	}
}

func TestLogAddE2O(t *testing.T) {
	log := NewLog()
	log.AddEvent(&Event{ID: "e1", Type: "Create"})
	log.AddObject(&Object{ID: "o1", Type: "order"})

	err := log.AddE2O("e1", "o1", "created")
	if err != nil {
		t.Fatalf("AddE2O error: %v", err)
	}
	if len(log.E2ORelations) != 1 {
		t.Fatalf("E2O count = %d, want 1", len(log.E2ORelations))
	}
}

func TestLogAddE2OInvalidEvent(t *testing.T) {
	log := NewLog()
	log.AddObject(&Object{ID: "o1", Type: "order"})

	err := log.AddE2O("bad_event", "o1", "q")
	if err == nil {
		t.Error("AddE2O with invalid event should return error")
	}
}

func TestLogAddO2O(t *testing.T) {
	log := NewLog()
	log.AddObject(&Object{ID: "o1", Type: "order"})
	log.AddObject(&Object{ID: "o2", Type: "item"})

	err := log.AddO2O("o1", "o2", "contains")
	if err != nil {
		t.Fatalf("AddO2O error: %v", err)
	}
	if len(log.O2ORelations) != 1 {
		t.Fatalf("O2O count = %d, want 1", len(log.O2ORelations))
	}
}

func TestLogAddO2OInvalidObject(t *testing.T) {
	log := NewLog()
	log.AddObject(&Object{ID: "o1", Type: "order"})

	err := log.AddO2O("o1", "bad_obj", "q")
	if err == nil {
		t.Error("AddO2O with invalid target should return error")
	}
}

func TestLogGetObjectsForEvent(t *testing.T) {
	log := NewLog()
	log.AddEvent(&Event{ID: "e1", Type: "Create"})
	log.AddObject(&Object{ID: "o1", Type: "order"})
	log.AddObject(&Object{ID: "o2", Type: "item"})
	_ = log.AddE2O("e1", "o1", "created")
	_ = log.AddE2O("e1", "o2", "referenced")

	objs := log.GetObjectsForEvent("e1")
	if len(objs) != 2 {
		t.Errorf("objects for event = %d, want 2", len(objs))
	}
}

func TestLogGetEventsForObject(t *testing.T) {
	log := NewLog()
	log.AddEvent(&Event{ID: "e1", Type: "Create", Timestamp: time.Now()})
	log.AddEvent(&Event{ID: "e2", Type: "Update", Timestamp: time.Now()})
	log.AddObject(&Object{ID: "o1", Type: "order"})
	_ = log.AddE2O("e1", "o1", "created")
	_ = log.AddE2O("e2", "o1", "modified")

	evts := log.GetEventsForObject("o1")
	if len(evts) != 2 {
		t.Errorf("events for object = %d, want 2", len(evts))
	}
}

func TestLogGetRelatedObjects(t *testing.T) {
	log := NewLog()
	log.AddObject(&Object{ID: "o1", Type: "order"})
	log.AddObject(&Object{ID: "o2", Type: "item"})
	_ = log.AddO2O("o1", "o2", "contains")

	related := log.GetRelatedObjects("o1")
	if len(related) != 1 {
		t.Fatalf("related objects = %d, want 1", len(related))
	}
	if related[0].ID != "o2" {
		t.Errorf("related object ID = %q, want %q", related[0].ID, "o2")
	}
}

func TestLogGetObjectsByType(t *testing.T) {
	log := NewLog()
	log.AddObject(&Object{ID: "o1", Type: "order"})
	log.AddObject(&Object{ID: "o2", Type: "item"})
	log.AddObject(&Object{ID: "o3", Type: "order"})

	orders := log.GetObjectsByType("order")
	if len(orders) != 2 {
		t.Errorf("orders count = %d, want 2", len(orders))
	}
}

func TestLogSetObjectAttribute(t *testing.T) {
	log := NewLog()
	log.AddObject(&Object{ID: "o1", Type: "order"})

	ts := time.Now()
	err := log.SetObjectAttribute("o1", "status", "shipped", "string", ts)
	if err != nil {
		t.Fatalf("SetObjectAttribute error: %v", err)
	}

	val := log.GetObjectAttributeAtTime("o1", "status", ts.Add(time.Second))
	if val != "shipped" {
		t.Errorf("attribute value = %v, want %q", val, "shipped")
	}
}

func TestLogProjectToCase(t *testing.T) {
	log := NewLog()
	log.AddEvent(&Event{ID: "e1", Type: "Create", Timestamp: time.Now()})
	log.AddEvent(&Event{ID: "e2", Type: "Ship", Timestamp: time.Now().Add(time.Hour)})
	log.AddObject(&Object{ID: "o1", Type: "order"})
	_ = log.AddE2O("e1", "o1", "created")
	_ = log.AddE2O("e2", "o1", "shipped")

	cases := log.ProjectToCase("order")
	if len(cases) != 1 {
		t.Fatalf("cases count = %d, want 1", len(cases))
	}
	if cases[0].CaseID != "o1" {
		t.Errorf("case ID = %q, want %q", cases[0].CaseID, "o1")
	}
}

func TestLogJSONRoundTrip(t *testing.T) {
	log := NewLog()
	log.AddEvent(&Event{ID: "e1", Type: "Create", Timestamp: time.Now().Truncate(time.Second)})
	log.AddObject(&Object{ID: "o1", Type: "order"})
	_ = log.AddE2O("e1", "o1", "created")

	data, err := log.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON error: %v", err)
	}

	restored, err := FromJSON(data)
	if err != nil {
		t.Fatalf("FromJSON error: %v", err)
	}
	if len(restored.Events) != 1 {
		t.Errorf("restored events = %d, want 1", len(restored.Events))
	}
	if len(restored.Objects) != 1 {
		t.Errorf("restored objects = %d, want 1", len(restored.Objects))
	}
}

func TestLogValidate(t *testing.T) {
	log := NewLog()
	log.AddEvent(&Event{ID: "e1", Type: "Create"})
	log.AddObject(&Object{ID: "o1", Type: "order"})
	_ = log.AddE2O("e1", "o1", "q")

	errs := log.Validate()
	if len(errs) != 0 {
		t.Errorf("validate errors = %v, want none", errs)
	}
}

func TestLogStats(t *testing.T) {
	log := NewLog()
	log.AddEvent(&Event{ID: "e1", Type: "Create"})
	log.AddEvent(&Event{ID: "e2", Type: "Ship"})
	log.AddObject(&Object{ID: "o1", Type: "order"})

	stats := log.Stats()
	if stats == nil {
		t.Fatal("Stats returned nil")
	}
}

// --- Discovery tests ---

func TestDiscoverOCDFG(t *testing.T) {
	log := NewLog()

	now := time.Now()
	log.AddEvent(&Event{ID: "e1", Type: "Create", Timestamp: now})
	log.AddEvent(&Event{ID: "e2", Type: "Ship", Timestamp: now.Add(time.Hour)})
	log.AddObject(&Object{ID: "o1", Type: "order"})
	_ = log.AddE2O("e1", "o1", "q")
	_ = log.AddE2O("e2", "o1", "q")

	dfg := DiscoverOCDFG(log)
	if dfg == nil {
		t.Fatal("DiscoverOCDFG returned nil")
	}

	edges := dfg.GetAllEdges()
	if len(edges) == 0 {
		t.Error("expected at least one edge")
	}
}

func TestOCDFGFilterByFrequency(t *testing.T) {
	dfg := NewOCDFG()
	if dfg == nil {
		t.Fatal("NewOCDFG returned nil")
	}

	filtered := dfg.FilterByFrequency(5)
	if filtered == nil {
		t.Fatal("FilterByFrequency returned nil")
	}
}

func TestOCDFGStats(t *testing.T) {
	log := NewLog()
	now := time.Now()
	log.AddEvent(&Event{ID: "e1", Type: "A", Timestamp: now})
	log.AddEvent(&Event{ID: "e2", Type: "B", Timestamp: now.Add(time.Hour)})
	log.AddObject(&Object{ID: "o1", Type: "order"})
	_ = log.AddE2O("e1", "o1", "q")
	_ = log.AddE2O("e2", "o1", "q")

	dfg := DiscoverOCDFG(log)
	stats := dfg.Stats()
	if stats == nil {
		t.Fatal("Stats returned nil")
	}
}

// --- Store tests (DuckDB in-memory) ---

func TestStoreCreateAndClose(t *testing.T) {
	store, err := NewStore("")
	if err != nil {
		t.Fatalf("NewStore error: %v", err)
	}
	defer store.Close()

	if store.DB() == nil {
		t.Error("DB() returned nil")
	}
}

func TestStoreInsertAndGetEvent(t *testing.T) {
	store, err := NewStore("")
	if err != nil {
		t.Fatalf("NewStore error: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	ev := &Event{
		ID:        "e1",
		Type:      "Create Order",
		Timestamp: time.Now().Truncate(time.Microsecond),
	}

	if err := store.InsertEvent(ctx, ev); err != nil {
		t.Fatalf("InsertEvent error: %v", err)
	}

	got, err := store.GetEvent(ctx, "e1")
	if err != nil {
		t.Fatalf("GetEvent error: %v", err)
	}
	if got.ID != "e1" {
		t.Errorf("event ID = %q, want %q", got.ID, "e1")
	}
	if got.Type != "Create Order" {
		t.Errorf("event type = %q, want %q", got.Type, "Create Order")
	}
}

func TestStoreInsertAndGetObject(t *testing.T) {
	store, err := NewStore("")
	if err != nil {
		t.Fatalf("NewStore error: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	obj := &Object{ID: "o1", Type: "order"}
	if err := store.InsertObject(ctx, obj); err != nil {
		t.Fatalf("InsertObject error: %v", err)
	}

	got, err := store.GetObject(ctx, "o1")
	if err != nil {
		t.Fatalf("GetObject error: %v", err)
	}
	if got.Type != "order" {
		t.Errorf("object type = %q, want %q", got.Type, "order")
	}
}

func TestStoreImportExport(t *testing.T) {
	store, err := NewStore("")
	if err != nil {
		t.Fatalf("NewStore error: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	log := NewLog()
	log.AddEvent(&Event{ID: "e1", Type: "Create", Timestamp: time.Now().Truncate(time.Microsecond)})
	log.AddObject(&Object{ID: "o1", Type: "order"})
	_ = log.AddE2O("e1", "o1", "created")

	if err := store.ImportLog(ctx, log); err != nil {
		t.Fatalf("ImportLog error: %v", err)
	}

	exported, err := store.ExportToLog(ctx)
	if err != nil {
		t.Fatalf("ExportToLog error: %v", err)
	}
	if len(exported.Events) != 1 {
		t.Errorf("exported events = %d, want 1", len(exported.Events))
	}
	if len(exported.Objects) != 1 {
		t.Errorf("exported objects = %d, want 1", len(exported.Objects))
	}
}

func TestStoreGetObjectsByType(t *testing.T) {
	store, err := NewStore("")
	if err != nil {
		t.Fatalf("NewStore error: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.InsertObject(ctx, &Object{ID: "o1", Type: "order"})
	_ = store.InsertObject(ctx, &Object{ID: "o2", Type: "item"})
	_ = store.InsertObject(ctx, &Object{ID: "o3", Type: "order"})

	orders, err := store.GetObjectsByType(ctx, "order")
	if err != nil {
		t.Fatalf("GetObjectsByType error: %v", err)
	}
	if len(orders) != 2 {
		t.Errorf("orders count = %d, want 2", len(orders))
	}
}

func TestStoreStats(t *testing.T) {
	store, err := NewStore("")
	if err != nil {
		t.Fatalf("NewStore error: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.InsertEvent(ctx, &Event{ID: "e1", Type: "A", Timestamp: time.Now()})
	_ = store.InsertObject(ctx, &Object{ID: "o1", Type: "order"})

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats error: %v", err)
	}
	if stats == nil {
		t.Fatal("Stats returned nil")
	}
}

func TestStoreProjectByObjectType(t *testing.T) {
	store, err := NewStore("")
	if err != nil {
		t.Fatalf("NewStore error: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.InsertEvent(ctx, &Event{ID: "e1", Type: "Create", Timestamp: time.Now()})
	_ = store.InsertObject(ctx, &Object{ID: "o1", Type: "order"})
	_ = store.InsertE2O(ctx, E2O{EventID: "e1", ObjectID: "o1", Qualifier: "q"})

	rows, err := store.ProjectByObjectType(ctx, "order")
	if err != nil {
		t.Fatalf("ProjectByObjectType error: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count == 0 {
		t.Error("expected at least one row from projection")
	}
}

func TestDiscoverOCDFGFromStore(t *testing.T) {
	store, err := NewStore("")
	if err != nil {
		t.Fatalf("NewStore error: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now()
	_ = store.InsertEvent(ctx, &Event{ID: "e1", Type: "Create", Timestamp: now})
	_ = store.InsertEvent(ctx, &Event{ID: "e2", Type: "Ship", Timestamp: now.Add(time.Hour)})
	_ = store.InsertObject(ctx, &Object{ID: "o1", Type: "order"})
	_ = store.InsertE2O(ctx, E2O{EventID: "e1", ObjectID: "o1", Qualifier: "q"})
	_ = store.InsertE2O(ctx, E2O{EventID: "e2", ObjectID: "o1", Qualifier: "q"})

	dfg, err := DiscoverOCDFGFromStore(ctx, store)
	if err != nil {
		t.Fatalf("DiscoverOCDFGFromStore error: %v", err)
	}
	if dfg == nil {
		t.Fatal("DiscoverOCDFGFromStore returned nil")
	}
}
