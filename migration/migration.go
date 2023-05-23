package migration

import (
	"encoding/binary"
	"errors"

	"github.com/NethermindEth/juno/db"
)

type revision func(db.DB) error

var revisions = []revision{
	revision0000,
}

func MigrateIfNeeded(targetDB db.DB) error {
	version, err := SchemaVersion(targetDB)
	if err != nil {
		return err
	}

	for i := version; i < uint64(len(revisions)); i++ {
		if err = revisions[i](targetDB); err != nil {
			return err
		}

		// revision returned with no errors, bump the version
		if err = targetDB.Update(func(txn db.Transaction) error {
			var versionBytes [8]byte
			binary.BigEndian.PutUint64(versionBytes[:], i+1)
			return txn.Set(db.SchemaVersion.Key(), versionBytes[:])
		}); err != nil {
			return err
		}
	}

	return nil
}

func SchemaVersion(targetDB db.DB) (uint64, error) {
	version := uint64(0)
	txn := targetDB.NewTransaction(false)
	err := txn.Get(db.SchemaVersion.Key(), func(bytes []byte) error {
		version = binary.BigEndian.Uint64(bytes)
		return nil
	})
	if err != nil && !errors.Is(err, db.ErrKeyNotFound) {
		return 0, db.CloseAndWrapOnError(txn.Discard, err)
	}

	return version, db.CloseAndWrapOnError(txn.Discard, nil)
}

// revision0000 makes sure the targetDB is empty
func revision0000(targetDB db.DB) error {
	return targetDB.View(func(txn db.Transaction) error {
		it, err := txn.NewIterator()
		if err != nil {
			return err
		}

		// not empty if valid
		if it.Next() {
			return db.CloseAndWrapOnError(it.Close, errors.New("initial DB should be empty"))
		}
		return it.Close()
	})
}
