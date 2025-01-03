package log

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

func TestPositionTracker(t *testing.T) {
	t.Parallel()

	t.Run("Set and Get AppliedPosition and ConsumerPosition", func(t *testing.T) {
		t.Parallel()

		tracker := NewPositionTracker()
		require.NoError(t, tracker.Set(AppliedPosition.Name, storage.LSN(5)))

		pos, err := tracker.Get(AppliedPosition.Name)
		require.NoError(t, err)
		require.Equal(t, storage.LSN(5), pos)

		require.NoError(t, tracker.Register(ConsumerPosition))
		require.NoError(t, tracker.Set(ConsumerPosition.Name, storage.LSN(10)))

		pos, err = tracker.Get(ConsumerPosition.Name)
		require.NoError(t, err)
		require.Equal(t, storage.LSN(10), pos)
	})

	t.Run("Set and Get single position multiple times", func(t *testing.T) {
		t.Parallel()

		tracker := NewPositionTracker()
		testPosition := storage.PositionType{Name: "TestPosition", ShouldNotify: false}
		require.NoError(t, tracker.Register(testPosition))

		for i := 1; i <= 3; i++ {
			require.NoError(t, tracker.Set(testPosition.Name, storage.LSN(i)))
			pos, err := tracker.Get(testPosition.Name)
			require.NoError(t, err)
			require.Equal(t, storage.LSN(i), pos)
		}
	})

	t.Run("Set and Get multiple positions", func(t *testing.T) {
		t.Parallel()

		tracker := NewPositionTracker()
		positions := []storage.PositionType{
			{Name: "Position1", ShouldNotify: false},
			{Name: "Position2", ShouldNotify: true},
		}
		values := []storage.LSN{5, 10}

		for i, posType := range positions {
			require.NoError(t, tracker.Register(posType))
			require.NoError(t, tracker.Set(posType.Name, values[i]))
		}

		for i, posType := range positions {
			pos, err := tracker.Get(posType.Name)
			require.NoError(t, err)
			require.Equal(t, values[i], pos)
		}
	})

	t.Run("Double register position type", func(t *testing.T) {
		t.Parallel()

		tracker := NewPositionTracker()
		testPosition := storage.PositionType{Name: "TestPosition", ShouldNotify: false}
		require.NoError(t, tracker.Register(testPosition))
		err := tracker.Register(testPosition)
		require.EqualError(t, err, "position type \"TestPosition\" already registered")
	})

	t.Run("Duplicated name register", func(t *testing.T) {
		t.Parallel()

		tracker := NewPositionTracker()
		testPosition1 := storage.PositionType{Name: "TestPosition", ShouldNotify: false}
		testPosition2 := storage.PositionType{Name: "TestPosition", ShouldNotify: true}

		require.NoError(t, tracker.Register(testPosition1))
		err := tracker.Register(testPosition2)
		require.EqualError(t, err, "position type \"TestPosition\" already registered")
	})

	t.Run("Ack unregistered position", func(t *testing.T) {
		t.Parallel()

		tracker := NewPositionTracker()
		posType := storage.PositionType{Name: "Unregistered", ShouldNotify: false}

		err := tracker.Set(posType.Name, storage.LSN(1))
		require.EqualError(t, err, "acknowledged an unregistered position type \"Unregistered\"")
	})

	t.Run("Get unregistered position", func(t *testing.T) {
		t.Parallel()

		tracker := NewPositionTracker()
		posType := storage.PositionType{Name: "Unregistered", ShouldNotify: false}

		_, err := tracker.Get(posType.Name)
		require.EqualError(t, err, "acknowledged an unregistered position type \"Unregistered\"")
	})

	t.Run("Range over positions", func(t *testing.T) {
		t.Parallel()

		tracker := NewPositionTracker()
		positions := []storage.PositionType{
			{Name: "Position1", ShouldNotify: false},
			{Name: "Position2", ShouldNotify: true},
		}
		values := []storage.LSN{5, 10}

		for i, posType := range positions {
			require.NoError(t, tracker.Register(posType))
			require.NoError(t, tracker.Set(posType.Name, values[i]))
		}

		trackedPositions := map[string]storage.LSN{}
		tracker.Each(func(name string, lsn storage.LSN) {
			trackedPositions[name] = lsn
		})

		require.Equal(t, storage.LSN(5), trackedPositions["Position1"])
		require.Equal(t, storage.LSN(10), trackedPositions["Position2"])
	})
}
