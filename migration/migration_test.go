package migration

import (
	"testing"

	"github.com/NethermindEth/juno/db"
	"github.com/NethermindEth/juno/db/pebble"
	"github.com/stretchr/testify/require"
)

func TestRevision0000(t *testing.T) {
	testDB := pebble.NewMemTest()
	t.Cleanup(func() {
		require.NoError(t, testDB.Close())
	})

	t.Run("empty DB", func(t *testing.T) {
		require.NoError(t, revision0000(testDB))
	})

	t.Run("non-empty DB", func(t *testing.T) {
		require.NoError(t, testDB.Update(func(txn db.Transaction) error {
			return txn.Set([]byte("asd"), []byte("123"))
		}))
		require.EqualError(t, revision0000(testDB), "initial DB should be empty")
	})
}

func TestMigrateIfNeeded(t *testing.T) {
	testDB := pebble.NewMemTest()
	t.Cleanup(func() {
		require.NoError(t, testDB.Close())
	})

	t.Run("Migration should happen on empty DB", func(t *testing.T) {
		require.NoError(t, MigrateIfNeeded(testDB))
	})

	version, err := SchemaVersion(testDB)
	require.NoError(t, err)
	require.NotEqual(t, 0, version)

	t.Run("subsequent calls to MigrateIfNeeded should not change the DB version", func(t *testing.T) {
		require.NoError(t, MigrateIfNeeded(testDB))
		postVersion, postErr := SchemaVersion(testDB)
		require.NoError(t, postErr)
		require.Equal(t, version, postVersion)
	})
}
